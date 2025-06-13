import time

from pprint import pprint
from elasticsearch import Elasticsearch
from constants import ES_HOST, ES_USERNAME, ES_PASSWORD

_es_client = None

def get_es_client(max_retries: int = 5, sleep_time: int = 5) -> Elasticsearch:
    global _es_client
    if _es_client is not None:
        return _es_client

    i = 0
    while i < max_retries:
        try:
            _es_client = Elasticsearch(
                ES_HOST,
                basic_auth=(ES_USERNAME, ES_PASSWORD)
            )
            pprint('Connected to Elasticsearch!')
            return _es_client
        except Exception as e:
            pprint(f"Error connecting to Elasticsearch: {e}")
            time.sleep(sleep_time)
            i += 1

    raise ConnectionError("Failed to connect to Elasticsearch after multiple attempts.")