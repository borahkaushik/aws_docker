import argparse, time, os

from ml_utils.date_utils import DATE_PARTITION

class DefaultArgs :
    APP_DATA_FOLDER: str = "/app/data"

    def generate_file_name(prefix, suffix:str = None, type:str = "parquet") :
        timeInMs = int(time.time() * 1000000)
        if suffix is not None :
            return "{}__{}__.{}".format(prefix, suffix, type)
        else :
            return "{}__{}__.{}".format(prefix, timeInMs, type)
    
class Args:
    required_params = []
    optional_params = []
    
    def __init__(self) :
        parser = argparse.ArgumentParser(description='Arg Processor')
            
        # Check for mandatory fields        
        for param in self.get_required_params() :
            parser.add_argument('--{}'.format(param), dest='{}'.format(param), required=True)

        for param in self.get_optional_params() :
            parser.add_argument('--{}'.format(param), dest='{}'.format(param), nargs='?', const=None, default=None)
        
        self.args = parser.parse_args()
        os.environ["AWS_DEFAULT_REGION"] = self.get_region()
        
    def generate_s3_url(self, out_type: str, filename:str = None):
        bucket = self.args.BUCKET if self.args.BUCKET is not None else "ml-stack.dev"
        url = "s3://{}/type={}/domain={}/{}".format(bucket, out_type, self.args.SEGMENT,DATE_PARTITION)
        if filename is not None :
            url = "/".join([url, filename])
        return url

    def get_region(self):
        return self.args.REGION if self.args.REGION is not None \
                else "us-east-1"

    @classmethod
    def get_required_params(cls) :
        return cls.required_params
    
    @classmethod
    def get_optional_params(cls) :
        return cls.optional_params
