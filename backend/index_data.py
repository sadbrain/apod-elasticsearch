import json
from pprint import pprint
from typing import List, Any

from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch
from tqdm import tqdm

from config import INDEX_NAME, INDEX_NAME_N_GRAMS
from utils import get_es_client

def index_data(documents: List[dict], use_n_gram_tokenizer: bool = False) -> List[dict]:
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es=es, use_n_gram_tokenizer=use_n_gram_tokenizer)
    _ = insert_documents(es=es, documents=documents, use_n_gram_tokenizer=use_n_gram_tokenizer)
    pprint(f"Indexed {len(documents)} documents into Elasticsearch index '{INDEX_NAME}'")

def create_index(es: Elasticsearch, use_n_gram_tokenizer: bool) -> dict:
    tokenizer = 'n_gram_tokenizer' if use_n_gram_tokenizer else 'standard'
    index_name = INDEX_NAME_N_GRAMS if use_n_gram_tokenizer else INDEX_NAME
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(
        index=index_name,
        settings={
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": tokenizer
                    }
                },
                "tokenizer": {
                    "n_gram_tokenizer": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 30,
                        "token_chars": [
                            "letter",
                            "digit"
                        ]
                    }
                },
            }
        })

def insert_documents(es: Elasticsearch, documents: List[dict], use_n_gram_tokenizer: bool) -> ObjectApiResponse[Any]:
    operations = []
    index_name = INDEX_NAME_N_GRAMS if use_n_gram_tokenizer else INDEX_NAME
    for doc in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": index_name}})
        operations.append(doc)
    return es.bulk(operations=operations)

if __name__ == '__main__':
    with open("./data/apod.json") as file:
        documents = json.load(file)
    # index_data(documents=documents)
    index_data(documents=documents, use_n_gram_tokenizer=True)