import os, awswrangler as wr, boto3, errno, logging
from datetime import datetime
from typing import Tuple, Type, Dict, List, Any

from ml_utils.date_utils import DATE_PARTITION

class S3Upload:

    def execute(self, local_file:str, s3_url:str):
        wr.s3.upload(path=s3_url, local_file=local_file, use_threads=True)
        print("Completed {} upload to {}".format(local_file, s3_url))

class S3Download:

    def __init__(self, region:str = "us-east-1", use_threads = True):
        self.client = boto3.client('s3', region_name=region)
        self.use_threads = use_threads

    def single_file(self, 
            s3_url:str, 
            local_file:str):
        wr.s3.download(path=s3_url,local_file=local_file, use_threads=self.use_threads)
        print("Completed {} download to {}".format(s3_url, local_file))

class S3Util:
    def __init__(self, region, url)-> None:
        self.s3 = boto3.client('s3', region_name=region)
        self.bucket, self.path = self.get_bucket_key(url)
        self.fileNames = []
        self.s3_urls = []
        print(self.path)

    def get_bucket_key(self, url) :
        if "//" in url :
            domain_keys_part = url.split("//")[1]
            if "/" in domain_keys_part :
                domain_keys = domain_keys_part.split("/")
                return domain_keys[0], "/".join(domain_keys[1:])
        raise ValueError("URL invalid {}".format(url))
    
    def get_s3_partition(self) -> str:
        return DATE_PARTITION
    
    
    def get_s3_filenames(self, maxkeys=100) -> List[Dict[str, str]]:
        
        contents = []
        try:
            query_kwargs: Dict[str, Any] = {
                'Bucket': self.bucket, 'MaxKeys': int(maxkeys), 'Prefix': self.path
            }
            response = self.s3.list_objects_v2(**query_kwargs)
            contents = response.get('Contents', [])
            while response['IsTruncated']:
                next_continuation_key = response['NextContinuationToken']
                response = self.s3.list_objects_v2(
                    **query_kwargs,
                    ContinuationToken=next_continuation_key
                )
                contents.extend(response.get('Contents'))
            if len(contents) < 1:
                raise Exception('Files not found for partition:{}'.format(self.path))
            self.fileNames = [content['Key'].split('/')[-1] for content in contents if len(content['Key'].split('/')[-1]) > 1]
            self.s3_urls = ['s3://{}/{}'.format(self.bucket, content['Key']) for content in contents if len(content['Key'].split('/')[-1]) > 1]
        except Exception as err:
            print(err)
        return self.fileNames, self.s3_urls