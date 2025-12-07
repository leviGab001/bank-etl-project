# src/main.py
"""
Main script to run the ETL pipeline end-to-end.

It uses default paths in utils. To change behavior, import functions and call them directly.
"""

import os
from .utils import setup_logger, ensure_dirs, log_progress, EXCHANGE_CSV, SAMPLE_HTML, OUTPUT_CSV, DB_PATH
from .extract import extract_from_html_file, extract_from_url
from .transform import transform
from .load import load_to_csv, load_to_db

logger = setup_logger()

def run_offline_test():
    """Run ETL using the sample HTML and local exchange CSV (for step-by-step testing)."""
    ensure_dirs()
    print("Running offline test ETL...")
    df = extract_from_html_file(SAMPLE_HTML)
    print("Extracted:", df.shape, "rows")
    df_t = transform(df, EXCHANGE_CSV)
    print("Transformed. Columns now:", list(df_t.columns))
    load_to_csv(df_t, OUTPUT_CSV)
    conn = load_to_db(df_t, DB_PATH)
    # simple query print
    cur = conn.cursor()
    cur.execute("SELECT Name, MC_USD_Billion, MC_GBP_Billion FROM Largest_banks LIMIT 5")
    rows = cur.fetchall()
    print("Sample rows from DB:")
    for r in rows:
        print(r)
    conn.close()
    log_progress("ETL offline test completed")
    print("Offline test finished. Outputs saved to:", OUTPUT_CSV, "and", DB_PATH)

def run_online(url: str):
    """Run ETL pulling from a real URL (requires internet)."""
    ensure_dirs()
    print("Running ETL from URL:", url)
    df = extract_from_url(url)  # may raise on network error
    df_t = transform(df, EXCHANGE_CSV)
    load_to_csv(df_t, OUTPUT_CSV)
    conn = load_to_db(df_t, DB_PATH)
    conn.close()
    log_progress("ETL online run completed")
    print("ETL completed. Outputs saved to:", OUTPUT_CSV, "and", DB_PATH)


if __name__ == "__main__":
    # Default offline test when executed directly
    run_offline_test()
