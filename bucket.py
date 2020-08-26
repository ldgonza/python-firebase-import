import os
import json
from google.cloud import storage

class FileSource:
    def __init__(self, bucket_name, prefix, delimiter):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.storage_client = None
        self.bucket = None

    def  __ensure_init(self):
        if self.bucket is None:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.get_bucket(self.bucket_name)
    
    def get_files(self, n=None):
        self.__ensure_init()
        return self.bucket.list_blobs(n, prefix=self.prefix, delimiter=self.delimiter)

    def rename(self, file, new_name):
        self.__ensure_init()
        new_blob = self.bucket.rename_blob(file, new_name)
        return new_blob
    
    def get(self, name):
        self.__ensure_init()
        return self.bucket.get_blob(name)
        
""" bucket_name = "gps-migration-apollo-qa"
prefix="done/"
delimiter="/"
source = FileSource(bucket_name, prefix, delimiter)
print(source.get("results/Vehicles/4.json").name)
 """
""" 
files = source.get_files(3)
print(json.loads(files[0].download_as_string()))
source.rename(files[0], "done/XXXX-2.json")
files[0].delete()

 """