from ml_utils.args_parser import DefaultArgs, Args
import os

class RerankerDefaultArgs:

    RERANKER_MODEL_PATH:str = "reranker.pt"
    MAX_SEQ_LENGTH = 256
    BATCH_SIZE = 4
    QUEUE_SIZE = 7500
    PRETRAINED_TOKENIZER_PATH: str = "roberta-base"
    NEURONCORE_GROUP_SIZES:int = 1
    THREAD_COUNT = 4
    RERANKER_MODEL_LOCAL_PATH = os.path.join(DefaultArgs.APP_DATA_FOLDER, RERANKER_MODEL_PATH)
    
    def get_exact_matches_local_file(suffix:str = None) :
        return DefaultArgs.generate_file_name("exact_matches", suffix)


class RerankerArgs(Args) :
    REGION: str = "REGION"
    SEGMENT = "SEGMENT"
    RERANKER_MODEL_S3_URL = "RERANKER_MODEL_S3_URL"
    NEURONCORE_GROUP_SIZES = "NEURONCORE_GROUP_SIZES"
    BUCKET = "BUCKET"
    EXACT_MATCHES_S3_URL = "EXACT_MATCHES_S3_URL"
    START_IDX = "START_IDX"
    END_IDX = "END_IDX"
    
    required_params = [SEGMENT, NEURONCORE_GROUP_SIZES, START_IDX, END_IDX]
    optional_params = [EXACT_MATCHES_S3_URL, REGION, BUCKET, RERANKER_MODEL_S3_URL]

    def get_exact_matches_s3_url(self, filename:str) :
        return self.args.EXACT_MATCHES_S3_URL if self.args.EXACT_MATCHES_S3_URL is not None \
            else self.generate_s3_url('exact_matches', filename)
    
    def get_reranker_model_s3_url(self) :
        print(self.args)
        return self.args.RERANKER_MODEL_S3_URL if self.args.RERANKER_MODEL_S3_URL is not None \
            else "s3://bungee-prod/models/reranker/reranker_neuron_v1_bs4.pt"
        