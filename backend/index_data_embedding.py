import json
from pprint import pprint
from typing import List, Any

import torch
from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch
from tqdm import tqdm

from config import INDEX_NAME_EMBEDDING
from utils import get_es_client
from sentence_transformers import SentenceTransformer

def index_data(documents: List[dict], model: SentenceTransformer) -> List[dict]:
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es=es, model=model)
    _ = insert_documents(es=es, documents=documents, model=model)
    pprint(f"Indexed {len(documents)} documents into Elasticsearch index '{INDEX_NAME_EMBEDDING}'")

def create_index(es: Elasticsearch, model: SentenceTransformer) -> dict:
    index_name = INDEX_NAME_EMBEDDING
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(
        index=index_name,
        mappings={
            "properties": {
                "title": {"type": "text"},
                "explanation": {"type": "text"},
                "date": {"type": "date"},
                "image_url": {"type": "keyword"},
                "author": {"type": "text"},
                "embedding": {
                    "type": "dense_vector"
                    # "dims": model.get_sentence_embedding_dimension()
                }
            }
        })

def insert_documents(es: Elasticsearch, documents: List[dict], model: SentenceTransformer) -> ObjectApiResponse[Any]:
    operations = []
    index_name = INDEX_NAME_EMBEDDING
    for doc in tqdm(documents, total=len(documents), desc="Indexing documents"):
        # operations.append({"index": {"_index": index_name}})
        # operations.append({
        #     **doc,
        #     'embedding': model.encode(doc['explanation']).tolist()
        # })
        embedding = model.encode(doc['explanation']).tolist()
        es.index(index=index_name, document={**doc, 'embedding': embedding})

    # return es.bulk(operations=operations)

if __name__ == '__main__':
    with open("./data/apod.json") as file:
        documents = json.load(file)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = SentenceTransformer('all-MiniLM-L6-v2').to(device)
    index_data(documents=documents, model=model)