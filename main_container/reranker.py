import torch, logging, traceback, torch_neuron, json, time, os, shutil, threading
from ml_utils.constants import MLConstants
from ml_utils.instance import ec2_shutdown
from fetch_athena import AthenaTask
from ml_utils.args_parser import DefaultArgs
from args import RerankerArgs, RerankerDefaultArgs

from transformers import RobertaTokenizer
from ml_utils.s3 import S3Download, S3Upload
from typing import Any, List, Tuple, Dict, cast, Union
from tqdm import tqdm
from ml_utils.multi_threading import MultiThreading
from ml_utils.file_utils import FileUtils as fu
import pyarrow.parquet as pq, numpy as np, math, pandas as pd, pyarrow as pa


class RerankerTask:
    
    def __init__(self,
        reranker_model_path: str,
        max_seq_length: int,
        batch_size: int
    ) -> None:

        self.tok = RobertaTokenizer.from_pretrained(RerankerDefaultArgs.PRETRAINED_TOKENIZER_PATH)
        self.base_tok_args = {
            "padding": "max_length",
            "truncation": True,
            "max_length": max_seq_length,
            "return_tensors": "pt"
        }

        self.model = torch.jit.load(reranker_model_path)
        self.batch_size = batch_size

    def boot(self):
        self.total_ids = 0
        self.processed_ids = 0
        self.processed_results = []

    def clean(self):
        del self.model

    def infer(self, batch_data, batch_id, task_id:int = 0):
        self.boot()
        print("Working on task ", task_id)
        """
        Runs model for the given input
        """
        
        while True:
            inputs = self.get_next_input(batch_data)
            if len(self.processed_results) >= len(batch_data) or len(inputs) == 0 :
                #pbar.close()
                logging.info("Completed....{}".format(self.processed_results))
                break
 
            items = self.convert_to_tensor(inputs)
            ids = torch.stack([inpt[1][0] for inpt in items])
            masks = torch.stack([inpt[1][1] for inpt in items])
            ctx = [inpt[0] for inpt in items]
            
            try :    
                result = self.model(*(ids, masks))
                for i, uuid in enumerate(ctx) :
                    self.processed_results.append((uuid[0],uuid[1],result[i].tolist()))
                
                if len(self.processed_results) % 5000 == 0:
                    print("{} Progress {}/{}".format(task_id,len(self.processed_results),len(batch_data)))
                    #pbar.update(len(self.processed_results)) 
                    
            except Exception as e:
                # Dont want to stop the process for other products
                logging.exception("Embedder: Failed due to {}".format(traceback.format_exc()))
                if 'NRTD' in traceback.format_exc() :
                    # Shutdown as the box leads to memory leak and take another instance
                    ec2_shutdown()
                raise e

        print("Completed on task ", task_id)
        return task_id, self.processed_results

    def get_next_input(self, batch_data) -> Any:
        if self.processed_ids * self.batch_size >= len(batch_data) :
            return []
        inputs = batch_data[self.processed_ids * self.batch_size : (self.processed_ids + 1) * self.batch_size]
        self.processed_ids = self.processed_ids + 1
        return inputs

    def convert_to_tensor(self, inputs) :
        items = []
        for raw_in in inputs:
            tok_args = {**self.base_tok_args}
            data = raw_in[1]
            #print(raw_in[0])
            tok_args["ids"] = data[0]
            tok_args["pair_ids"] = data[1]
            item = self.tok.prepare_for_model(**tok_args)
            item = cast(Dict[str, torch.Tensor], item)

            items.append((raw_in[0], (item["input_ids"], item["attention_mask"])))
        return items
        
class Reranker:

    def __init__(self, env: RerankerArgs) -> None:
        self.start_idx = int(env.args.START_IDX)
        self.end_idx = int(env.args.END_IDX)
        self.segment = env.args.SEGMENT
        self.suffix = "{}_{}".format(self.start_idx, self.end_idx)

        self.reranker_model_s3_url = env.get_reranker_model_s3_url()
        self.reranker_model_local_path = RerankerDefaultArgs.RERANKER_MODEL_LOCAL_PATH

        self.exact_matches_local_path = RerankerDefaultArgs.get_exact_matches_local_file(self.suffix)
        self.exact_matches_s3_url = env.get_exact_matches_s3_url(self.exact_matches_local_path)
        
        self.batch_size = RerankerDefaultArgs.BATCH_SIZE
        self.queue_size = RerankerDefaultArgs.QUEUE_SIZE
        self.thread_count = int(int(env.args.NEURONCORE_GROUP_SIZES)  * 1.25)
        self.cores = int(env.args.NEURONCORE_GROUP_SIZES)
        self.max_seq_length = RerankerDefaultArgs.MAX_SEQ_LENGTH
        
        self.current_queue_index = 0
        self.no_of_paddings = 0
        
    def boot(self) :
        s3_download = S3Download()
        s3_download.single_file(self.reranker_model_s3_url, self.reranker_model_local_path)
        
    def process(self) -> None:

        self.boot()
        tqdm_args: Dict[Tuple[str,str], Tuple[Union[str, int], Union[str, int]]] = {
            "desc":"Reranker Progress ", "mininterval": 2, "total": self.end_idx - self.start_idx
        }
        pbar = tqdm(**tqdm_args)
        
        tasks = [RerankerTask(self.reranker_model_local_path, self.max_seq_length, self.batch_size) for _ in range(self.cores)]
        m = MultiThreading(self.thread_count, tasks)        
        result_df = pd.DataFrame()
        total_recs = 0
        
        for start, end in self.split_into_chunks():
            print("Current progress {} - {}".format(start,end))
            task = AthenaTask(self.segment, start, end)
            df_iter = task.fetch_matches(self.batch_size * self.queue_size)
            
            for batch_id, df in enumerate(df_iter) :
                total_recs = total_recs + len(df)
                all_batches = self.get_all_batches(df)
                for batch_data in all_batches:
                    
                    results = m.submit(batch_data, batch_id)
                    if len(results) > 0 :
                        for res in results :
                            result_df = pd.concat([result_df, pd.DataFrame(res)], ignore_index=True)
                            pbar.update(len(result_df))
                        
        results = m.wait_for_all_processes()

        for res in results :
            result_df = pd.concat([result_df, pd.DataFrame(res)], ignore_index=True)
            pbar.update(len(result_df))
            
        pbar.close()
        result_df = self.remove_padding(result_df)
        print("Total records {}, processed {}".format(total_recs, len(result_df)))
        for task in tasks:
            task.clean()
            
        self.upload(result_df)


    def upload(self, result_df:pd.DataFrame) -> None :
        result_df.rename(columns={result_df.columns[0]:'uuid_a',result_df.columns[1]:'uuid_b', result_df.columns[2]:'score'}, inplace=True)
        print("Total records {}".format(len(result_df)))
        id_exact_matches_schema = MLConstants.EXACT_MATCHES_SCHEMA
        pq_writer = pq.ParquetWriter(self.exact_matches_local_path, schema=pa.schema(id_exact_matches_schema), compression='SNAPPY') 
        pq_writer.write_table(pa.Table.from_pandas(result_df))
        pq_writer.close()

        s3_upload = S3Upload()
        s3_upload.execute(local_file=self.exact_matches_local_path, s3_url=self.exact_matches_s3_url)


    def get_all_batches(self, df: pd.DataFrame) -> Any:
        all_batches = [] 
        for i in range(0, df.shape[0], self.queue_size):
            frame = df[i:i+self.queue_size]
            #print("Frame size {}".format(len(frame)))
            batch = []
            for _, id_a, id_b, input_id_a, input_id_b in frame.itertuples() :
                batch.append(((id_a,id_b), (list(input_id_a),list(input_id_b))))

            all_batches.append(batch)    
        self.add_padding(batch)

        logging.info("Total batches for processing {} ".format(len(all_batches)))
        print("Total batches for processing {}".format(len(all_batches)))
        return all_batches

    def add_padding(self, last_batch: List[Tuple[int,float]]):
        if len(last_batch) % self.batch_size != 0 :
            extra_len = self.batch_size - (len(last_batch) % self.batch_size)
            last_obj = last_batch[len(last_batch) - 1]
            print("Padding added {} times of {}".format(extra_len, last_obj[0]))
            last_batch.extend([last_obj] * extra_len)
            self.no_of_paddings = extra_len
    
    def remove_padding(self, df:pd.DataFrame) :
        if self.no_of_paddings > 0 :
            print("Found Paddings... so removing last {} records".format(self.no_of_paddings))
            df = df[:len(df)-self.no_of_paddings]
            
        return df
        
    def split_into_chunks(self) :
        chunk_size = int((self.thread_count * self.queue_size * self.batch_size)/2) 
        total_products = self.end_idx
        tasks = []
        for i in range(self.start_idx, total_products, chunk_size):
            tasks.append((i, i + chunk_size if i + chunk_size < total_products else total_products))
        
        print(tasks)
        return tasks