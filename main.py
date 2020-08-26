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
proc_count = 1

# #################################################
def fill_input_queue(q):
    file_source = FileSource(bucket_name, prefix, delimiter)
    files = file_source.get_files()
    for file in files:
        q.put(file.name)

# #################################################

def do_process(input_queue: Queue, output_queue: Queue):
    file_source = FileSource(bucket_name, prefix, delimiter)
    importer = Importer(credentials)

    try:
        while True:
            file_name = input_queue.get(True, 1)
            file = file_source.get(file_name)
            output_queue.put("Processing " + file_name)

            try:
                contents = json.loads(file.download_as_string())
                importer.import_to_firebase(contents)
                file.delete()
            except:
                file_source.rename(file, "/errors/" + file.name)

            output_queue.put("Done " + file_name)
    except queues.Empty:
        return
    except Exception as e:
        output_queue.put("Error with " + file_name + ": " + str(e))
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