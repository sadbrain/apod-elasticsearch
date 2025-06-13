from typing import Any, Coroutine
from log_config import setup_logging
import logging

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse

from config import INDEX_NAME
from utils import get_es_client

setup_logging()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True)

@app.get("/api/v1/regular_search")
async def search(search_query: str, skip: int = 0, limit: int = 10) -> dict:
    es = get_es_client(max_retries=5, sleep_time=5)
    res = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "multi_match": {
                    "query": search_query,
                    "fields": ["title^2", "explanation"],
                }
            },
            "from": skip,
            "size": limit
        },
        filter_path=["hits.hits._source", "hits.hits._score"]
    )
    hits = res.get("hits", {}).get("hits", [])
    return {"hits": hits}

@app.get("/api/v1/get_docs_per_year_count", response_model=None)
async def get_docs_per_year_count(search_query: str):
    try:
        es = get_es_client(max_retries=1, sleep_time=0)
        query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "fields": ["title^2", "explanation"]
                        }
                    }
                ]
            }
        }
        res = es.search(
            index=INDEX_NAME,
            body={
                "query": query,
                "aggs": {
                    "docs_per_year": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "year",
                            "format": "yyyy",
                        }
                    }
                },
                "size": 0,
            },
            filter_path=["aggregations.docs_per_year"]
        )
        return {"docs_per_year": extract_docs_per_year(res)}
    except Exception as e:
        # Return plain text in HTML format
        return HTMLResponse(content=f"Internal Server Error: {e}", status_code=500)

def extract_docs_per_year(res: dict) -> dict:
    aggregations = res.get("aggregations", {})
    doc_per_year = aggregations.get("docs_per_year", {})
    buckets = doc_per_year.get("buckets", [])
    return {bucket["key_as_string"]: bucket["doc_count"] for bucket in buckets}

@app.on_event("startup")
async def startup_event():
    logging.getLogger(__name__).info("[event] FastAPI started.")

@app.on_event("shutdown")
async def shutdown_event():
    logging.getLogger(__name__).info("[event] FastAPI shutting down.")
