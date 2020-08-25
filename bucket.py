import os
from google.cloud import storage

bucket_name = "gps-migration-apollo-qa"
prefix="results/Vehicles/"
delimiter="/"

class FileSource:
    def __init__(self, bucket_name, prefix, delimiter):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.storage_client = None
        self.bucket = None

    def get_files(self, n=1):
        if self.bucket is None:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.get_bucket(self.bucket_name)

        blobs = list(self.bucket.list_blobs(n, prefix=self.prefix, delimiter=self.delimiter))
        return blobs
        

source = FileSource(bucket_name, prefix, delimiter)
files = source.get_files(3)
print(files)