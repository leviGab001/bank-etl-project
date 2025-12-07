# src/extract.py
"""
Extraction module:
 - extract_from_url(url): download page and extract table
 - extract_from_html_file(path): for offline testing with saved HTML
Both functions return a pandas.DataFrame with columns:
   Name (str), MC_USD_Billion (float)
"""

import re
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

from .utils import log_progress

# Default user agent for polite requests
HEADERS = {"User-Agent": "banks-etl-bot/1.0 (+https://example.org)"}

def _clean_mc_val(x):
    """
    Convert raw market cap cell (string like '1,200' or '$1,200' or '1,200 [1]') to float.
    Returns NaN when conversion fails.
    """
    if pd.isna(x):
        return np.nan
    s = str(x)
    s = re.sub(r"\[.*?\]", "", s)  # drop footnotes like [1]
    s = s.replace("US$", "").replace("$", "")
    # remove any characters except digits, dot and minus
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s == "":
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan

def extract_from_html_string(html: str) -> pd.DataFrame:
    """
    Parse HTML string and try to find the 'By market capitalization' table.
    If that section is not found, fallback to pandas.read_html on the whole HTML.
    """
    soup = BeautifulSoup(html, "lxml")
    # Try to find heading that contains 'By market capitalization'
    heading = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and "By market capitalization" in tag.text)
    table_html = None
    if heading:
        sib = heading.find_next_sibling()
        while sib:
            if sib.name == "table":
                table_html = str(sib)
                break
            sib = sib.find_next_sibling()
    if table_html:
        tables = pd.read_html(table_html)
        if len(tables) == 0:
            raise RuntimeError("No tables found under the 'By market capitalization' heading.")
        df = tables[0]
    else:
        # fallback: parse all tables and pick a candidate
        tables = pd.read_html(html, flavor="lxml")
        # pick the first table with 'market' in col names
        candidate = None
        for t in tables:
            cols = [str(c).lower() for c in t.columns.astype(str)]
            if any("market" in c for c in cols) or any("market" in str(v).lower() for v in cols):
                candidate = t
                break
        if candidate is None:
            # last resort: largest table
            candidate = max(tables, key=lambda x: x.shape[0])
        df = candidate.copy()

    # Detect name column and market-cap column heuristically
    cols = [str(c).strip() for c in df.columns]
    name_col = None
    mc_col = None
    for i, c in enumerate(cols):
        lc = c.lower()
        if any(k in lc for k in ["name", "bank"]):
            name_col = df.columns[i]
        if ("market" in lc and ("cap" in lc or "capital" in lc)) or ("usd" in lc and "billion" in lc) or ("market cap" in lc):
            mc_col = df.columns[i]

    # fallback strategies
    if name_col is None:
        name_col = df.columns[0]
    if mc_col is None:
        # prefer a column with numeric-like values
        for c in df.columns:
            if c == name_col:
                continue
            sample = df[c].astype(str).head(10).tolist()
            numeric_like = sum(1 for v in sample if re.search(r"[0-9]", v))
            if numeric_like >= 1:
                mc_col = c
                break
    if mc_col is None:
        raise RuntimeError("Could not find a market-cap column automatically. Inspect the table.")

    # Build the result dataframe
    res = pd.DataFrame()
    res["Name"] = df[name_col].astype(str).str.strip()
    res["MC_USD_Billion"] = df[mc_col].apply(_clean_mc_val)
    # Drop rows where market cap is missing
    res = res.dropna(subset=["MC_USD_Billion"]).reset_index(drop=True)

    log_progress("Data extraction complete")
    return res

def extract_from_html_file(path: str) -> pd.DataFrame:
    """Read local HTML file and extract using extract_from_html_string."""
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    return extract_from_html_string(html)

def extract_from_url(url: str, timeout: int = 30) -> pd.DataFrame:
    """Download URL and extract table. Raises for HTTP errors."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return extract_from_html_string(resp.text)
