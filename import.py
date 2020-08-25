import json
import firebase_admin
import google.cloud
from firebase_admin import credentials, firestore


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

def do_import_to_firebase(contents, store):
    result = []
    for collection_name in contents:
        data = contents[collection_name]
        for batched_data in batch_data(data, 499):
            r = import_batch(collection_name, batched_data, store)
            result.append(r)
    return result

class Importer:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path

    def start(self):
        self.cred = credentials.Certificate(self.credentials_path)
        self.app = firebase_admin.initialize_app(self.cred)
        self.store = firestore.client()

    def import_to_firebase(self, contents):
        return do_import_to_firebase(contents, self.store)