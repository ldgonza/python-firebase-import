import time
import json
from firebase_import import Importer
from process import process
from bucket import FileSource
from multiprocessing import Process, Queue, queues

# #################################################

credentials = "./sa-key.json"
bucket_name = "gps-migration-apollo-qa"
prefix="results/Vehicles/"
delimiter="/"
proc_count = 3
num_files = None

file_source = FileSource(bucket_name, prefix, delimiter)
files = file_source.get_files(num_files)

def get_number(file):
  name = file.name.split("/").pop()
  number = name.split(".")[0]
  return int(number)

max = 0

for file in files:
    n = get_number(file)

    print(".", end="")
    if max < n:
        max = n

print(max)