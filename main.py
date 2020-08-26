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
num_files = 1

# #################################################

def fill_input_queue(q):
    file_source = FileSource(bucket_name, prefix, delimiter)
    files = file_source.get_files(num_files)
    for file in files:
        q.put(file.name)

# #################################################

def get_time():
    return str(int(time.time() * 10000000))

def do_process(input_queue: Queue, output_queue: Queue):
    file_source = FileSource(bucket_name, prefix, delimiter)
    importer = Importer(credentials)

    try:
        while True:
            file_name = input_queue.get(True, 1)
            file = file_source.get(file_name)
            output_queue.put(get_time() + " - Processing " + file_name)

            try:
                contents = json.loads(file.download_as_string().decode("utf-8"))
                importer.import_to_firebase(contents)
                file.delete()
                output_queue.put(get_time() + " - Done " + file_name)
            except Exception as e:
                file_source.rename(file, "errors/" + file_name)
                output_queue.put(get_time() + " - Error " + file_name + ": " + str(e))
            
    except queues.Empty:
        return
    except Exception as e:
        output_queue.put(get_time() + " - Error with " + file_name + ": " + str(e))
        raise e

# #################################################

input_queue = Queue()
output_queue = Queue()

fill_input_queue(input_queue)

processes = []
for i in range(proc_count):
    p = Process(target=do_process, args=(input_queue,output_queue))
    p.start()
    processes.append(p)

running = True
while running:
    running = False
    for p in processes:
        running = running or p.is_alive()
    
    while not output_queue.empty():
        print(output_queue.get())