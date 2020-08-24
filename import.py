import csv
import firebase_adminimport google.cloud
from firebase_admin import credentials, firestore
cred = credentials.Certificate("./ServiceAccountKey.json")
app = firebase_admin.initialize_app(cred)
store = firestore.client()
file_path = "CSV_FILE_PATH"
collection_name = "COLLECTION_TO_ADD_TO"

def batch_data(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def get_data_item(item, data_type):
	# Add other data types you want to handle here
    if data_type == 'int':
        return int(item)
    elif data_type == 'bool':
        return bool(item)
    else:
        return item

data = []
headers = []
data_types = []
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            for header in row:
                headers.append(header)
            line_count += 1
        elif line_count == 1:
            for data_type in row:
                data_types.append(data_type)
            line_count += 1
        else:
            obj = {}
            for idx, item in enumerate(row):
                obj[headers[idx]] = get_data_item(item, data_types[idx])
            data.append(obj)
            line_count += 1
    print(f'Processed {line_count} lines.')
for batched_data in batch_data(data, 499):
    batch = store.batch()
    for data_item in batched_data:
        doc_ref = store.collection(collection_name).document()
        batch.set(doc_ref, data_item)
    batch.commit()
print('Done')