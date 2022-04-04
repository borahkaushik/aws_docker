from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, FIRST_COMPLETED, wait
import concurrent.futures
import logging
from threading import Event,Thread 
from typing import Any

class MultiThreading:
    def __init__(self,max_workers:int, tasks:Any = []):
        self.max_workers = max_workers
        self.executors = ThreadPoolExecutor(max_workers=self.max_workers)
        self.futures = set()
        self.available_tasks = tasks
        self.available_ids = [k for k in range(len(tasks))]
        print("Tasks ",self.available_tasks)
        print("Init Ids ",self.available_ids)
        #self.working_tasks = []
        #self.results = []
        #print(self.available_tasks)
        

    def submit(self, *args, **kargs):
        done = self.wait_for_process_release()
        print("Submit ids ",self.available_ids)
        id = self.available_ids.pop()
        kargs.update({"task_id" : id})
        self.futures.add(self.executors.submit(self.available_tasks[id].infer,*args, **kargs))
        return done

    def wait_for_process_release(self):
        temp_result = []
        while len(self.futures) >= self.max_workers or len(self.available_ids) == 0:
            print("Before wait ids ",self.available_ids)
            done, self.futures = wait(self.futures, return_when=FIRST_COMPLETED)
            for fut in done:
                #print("Task completed ", len(fut.result()))
                #self.results.append(fut.result())
                id, result = fut.result()
                temp_result.append(result)
                self.available_ids.append(id)
                del fut
            print("After wait ids ",self.available_ids)
            #self.futures = set()
        #print("Wait for process release ", len(temp_result))
        return temp_result

    def wait_for_all_processes(self):
        temp_result = []
        print("Before all wait ids ",self.available_ids)
        for fut in concurrent.futures.as_completed(self.futures):
            logging.info(fut)
            #self.results.append(fut.result())
            id, result = fut.result()
            temp_result.append(result)
            self.available_ids.append(id)
            ex = fut.exception()
            if ex is not None:
                try:
                    raise ex
                except Exception as e:
                    logging.exception("Loader Process Failed",e)
            del fut
        print("After all wait ids ",self.available_ids)
        del self.available_ids
        del self.available_tasks
        self.executors.shutdown()
        return temp_result
    
