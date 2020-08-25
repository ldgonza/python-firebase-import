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

    def get_files(self, n=1):
        if self.bucket is None:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.get_bucket(self.bucket_name)

        return self.bucket.list_blobs(n, prefix=self.prefix, delimiter=self.delimiter)

    def rename(self, file, new_name):
        new_blob = self.bucket.rename_blob(file, new_name)
        return new_blob
        


# bucket_name = "gps-migration-apollo-qa"
# prefix="done/"
# delimiter="/"
# source = FileSource(bucket_name, prefix, delimiter)
# files = source.get_files(3)
# print(json.loads(files[0].download_as_string()))
# source.rename(files[0], "done/XXXX-2.json")


# files[0].delete()
