import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers = [logging.StreamHandler()]
    if os.getenv("ENV") == "production":
        handlers.append(RotatingFileHandler("logs/app.log", maxBytes=10_000_000, backupCount=3))

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
    )