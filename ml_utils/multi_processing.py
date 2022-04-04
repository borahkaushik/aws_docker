from concurrent.futures import ProcessPoolExecutor, FIRST_COMPLETED, wait
import concurrent.futures
import logging, random

class MultiProcessing:
    def __init__(self,max_workers:int, name:str=None):
        self.max_workers = max_workers
        self.executors = ProcessPoolExecutor(max_workers=self.max_workers)
        self.futures = set()
        self.name = name if name else "Multiprocessing - " + str(random.randint(1, 10000))

    def submit(self, fn, *args, **kargs):
        done = self.wait_for_process_release()
        self.futures.add(self.executors.submit(fn,*args, **kargs))
        return done

    def wait_for_process_release(self):
        results = []
        while len(self.futures) >= self.max_workers:
            done, self.futures = wait(self.futures, return_when=FIRST_COMPLETED)
            for fut in done:
                results.append(fut.result())
                del fut
        return results

    def wait_for_all_processes(self):
        results = []
        for fut in concurrent.futures.as_completed(self.futures):
            logging.info(fut)
            results.append(fut.result())
            ex = fut.exception()
            if ex is not None:
                try:
                    raise ex
                except Exception as e:
                    logging.exception("Loader Process Failed",e)
            del fut
        self.executors.shutdown()
        return results            