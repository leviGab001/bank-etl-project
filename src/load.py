# src/load.py
"""
Loading module:
 - load_to_csv(df, path): save dataframe to CSV
 - load_to_db(df, db_path, table_name): save to sqlite db and return connection
"""

import sqlite3
import pandas as pd
from .utils import log_progress

def load_to_csv(df: pd.DataFrame, path: str):
    """Save DataFrame to CSV (index=False)."""
    df.to_csv(path, index=False, encoding="utf-8")
    log_progress("Data saved to CSV")

def load_to_db(df: pd.DataFrame, db_path: str, table_name: str = "Largest_banks"):
    """Save DataFrame to SQLite. Returns sqlite3.Connection (open)."""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    log_progress("Data loaded to SQLite DB")
    return conn
