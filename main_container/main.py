import os,time,logging,sys
import traceback
from reranker import Reranker
from args import RerankerArgs

"""
Entry point for embedder container
Retrive value from mandtory & optional parameters from ENV
Calls embedder to complete generating embeddings
"""
if __name__ == "__main__":
    logging.info("Embedder container started - {}".format(os.getpid()))
    
    try:

        rank_args = RerankerArgs()
        print("Passed Args {}".format(rank_args.args))

        os.environ[RerankerArgs.NEURONCORE_GROUP_SIZES] = ",".join(['1'] * int(rank_args.args.NEURONCORE_GROUP_SIZES))

        start_time = time.time()
        ranker = Reranker(rank_args)
        ranker.process()
        logging.info("--- %s seconds ---" % (time.time() - start_time))

    except Exception as e:
        traceback.print_exc()
        logging.info(str(e))
        sys.exit(2)

    logging.info("Reranker container completed") 