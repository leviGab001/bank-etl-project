# src/utils.py
"""
Utility helpers for the ETL project:
 - setup_logger: configure file logger
 - ensure_dirs: makes sure required folders exist
 - constants for paths
"""
  
     
     
       
import os
import logging
from datetime import datetime

# Base paths 
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "code_log.txt")

# Files used by default
EXCHANGE_CSV = os.path.join(DATA_DIR, "exchange_rate.csv")
SAMPLE_HTML = os.path.join(DATA_DIR, "sample_banks.html")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "Largest_banks_data.csv")
DB_PATH = os.path.join(OUTPUT_DIR, "Banks.db")

# Ensure directories exist
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

# Logger setup: append mode, simple timestamped messages
def setup_logger():
    ensure_dirs()
    logger = logging.getLogger("bank_etl")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers on repeated imports
    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s : %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

# small helper to append a single log message (used by modules)
def log_progress(message: str):
    logger = setup_logger()
    logger.info(message)
