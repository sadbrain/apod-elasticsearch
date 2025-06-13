import json
from pprint import pprint
from typing import List, Any

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch
from tqdm import tqdm

from config import INDEX_NAME
from utils import get_es_client

def index_data(documents: List[dict]) -> List[dict]:
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es=es)
    _ = insert_documents(es=es, documents=documents)
    pprint(f"Indexed {len(documents)} documents into Elasticsearch index '{INDEX_NAME}'")

def create_index(es: Elasticsearch) -> dict:
    es.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
    return es.indices.create(index=INDEX_NAME)

def insert_documents(es: Elasticsearch, documents: List[dict]) -> ObjectApiResponse[Any]:
    operations = []
    for doc in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": INDEX_NAME}})
        operations.append(doc)
    return es.bulk(operations=operations)

if __name__ == '__main__':
    with open("./data/apod.json") as file:
        documents = json.load(file)
    index_data(documents=documents)