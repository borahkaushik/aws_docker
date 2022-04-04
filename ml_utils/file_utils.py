import time

class FileUtils:
    def get_file_index(s3_url : str) -> str :
        data = s3_url.split("__")
        return data[1] if len(data) > 2 else int(time.time() * 1000000)