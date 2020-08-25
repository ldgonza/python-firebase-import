import json
import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore

cred = credentials.Certificate("./sa-key.json")
app = firebase_admin.initialize_app(cred)
store = firestore.client()
file_path = "./data.json"

def batch_data(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def import_batch(collection_name, batched_data, store):
    batch = store.batch()
    for data_item in batched_data:
        doc = data_item.copy()
        id = None
        if "id" in doc:
            id = doc["id"]
            del doc['id']
        
        doc_ref = store.collection(collection_name).document(id)
        batch.set(doc_ref, doc)

    return batch.commit()

def import_to_firebase(contents, store):
    for collection_name in contents:
        data = contents[collection_name]
        for batched_data in batch_data(data, 499):
            result = import_batch(collection_name, batched_data, store)

    print('Done')

with open(file_path) as json_file:
    contents = json.load(json_file)
    import_to_firebase(contents, store)

