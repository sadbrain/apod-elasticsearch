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
async def search(search_query: str, skip: int = 0, limit: int = 10, year: str | None = None) -> dict:
    es = get_es_client(max_retries=1, sleep_time=0)
    query =  {
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
    if year:
        query["bool"]["filter"] = [
            {
                "range": {
                    "date": {
                        "gte": f"{year}-01-01",
                        "lte": f"{year}-12-31",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        ]
    res = es.search(
        index=INDEX_NAME,
        query=query,
        from_=skip,
        size=limit,
        filter_path=[
            "hits.hits._source",
            "hits.hits._score",
            "hits.total"
        ]
    )
    total_hits = get_total_hits(res)
    max_pages = calculate_max_page(total_hits, limit)
    hits = res.get("hits", {}).get("hits", [])
    return {
        "max_pages": max_pages,
        "hits": hits
    }
def get_total_hits(res: dict) -> int:
    return res.get("hits", {}).get("total", {}).get("value", 0)

def calculate_max_page(total_hits: int, limit: int) -> int:
    if total_hits == 0:
        return 0
    return (total_hits + limit - 1) // limit
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
