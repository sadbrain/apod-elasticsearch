import os
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_USERNAME = os.getenv("ES_USERNAME")
ES_PASSWORD = os.getenv("ES_PASSWORD")