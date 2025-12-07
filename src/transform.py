# src/transform.py
"""
Transformation module:
 - transform(df, exchange_csv): read exchange CSV and add MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
The exchange CSV format should be 2 columns: Currency, Rate (1 USD -> X currency)
Example:
Currency,Rate
GBP,0.81
EUR,0.93
INR,82.5
"""

import pandas as pd
from .utils import log_progress
from typing import Dict

def _load_rates(exchange_csv: str) -> Dict[str, float]:
    """Return a mapping currency_code -> rate (float)."""
    rates_df = pd.read_csv(exchange_csv)
    # assume first two columns are (Currency, Rate) but support varied names
    col0 = rates_df.columns[0]
    col1 = rates_df.columns[1]
    rates = {str(rates_df.loc[i, col0]).strip(): float(rates_df.loc[i, col1]) for i in rates_df.index}
    # normalize keys to uppercase
    rates = {k.upper(): v for k, v in rates.items()}
    return rates

def transform(df: pd.DataFrame, exchange_csv: str):
    rates = _load_rates(exchange_csv)
    def get_rate(code):
        c = code.upper()
        if c in rates:
            return rates[c]
        # try partial matches if e.g. "GBP - British Pound"
        for k in rates:
            if c in k:
                return rates[k]
        raise KeyError(f"Rate for currency '{code}' not found in {exchange_csv}")

    gbp = get_rate("GBP")
    eur = get_rate("EUR")
    inr = get_rate("INR")

    out = df.copy()
    usd = out["MC_USD_Billion"].astype(float)
    out["MC_GBP_Billion"] = (usd * gbp).round(2)
    out["MC_EUR_Billion"] = (usd * eur).round(2)
    out["MC_INR_Billion"] = (usd * inr).round(2)

    log_progress("Data transformation complete")
    return out
