# src/stock_analyzer/data.py
from __future__ import annotations
from typing import Optional
import pandas as pd
import yfinance as yf

WANTED_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

def _flatten_columns(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        # Prefer inner level (fields) for single ticker
        try:
            # If there’s only one symbol level, drop it
            if len(pd.unique(df.columns.get_level_values(0))) == 1:
                df = df.droplevel(0, axis=1)
            else:
                df = df.xs(ticker, axis=1, level=0)
        except Exception:
            # Fallback: join tuples → "SPY Adj Close"
            df.columns = [" ".join(map(str, c)).strip() for c in df.columns]
    else:
        df.columns = [str(c).strip().title() for c in df.columns]

    # Map any "SPY Adj Close" → "Adj Close" etc.
    cols = list(df.columns)
    rename_map = {}
    for wanted in WANTED_COLS:
        if wanted in cols:
            continue
        for c in cols:
            if c.endswith(wanted):
                rename_map[c] = wanted
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    # Keep standard fields if present
    keep = [c for c in WANTED_COLS if c in df.columns]
    if keep:
        df = df[keep]
    return df

def fetch_prices(
    ticker: str,
    period: Optional[str] = "5y",
    interval: str = "1d",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Returns OHLCV with guaranteed 'Adj Close' present.
    We use auto_adjust=True so 'Close' is already adjusted; we then mirror it to 'Adj Close'.
    """
    kwargs = dict(interval=interval, progress=False, group_by="column", auto_adjust=True)
    if start or end:
        df = yf.download(ticker, start=start, end=end, **kwargs)
    else:
        df = yf.download(ticker, period=period, **kwargs)

    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker}. Check ticker/date range/interval.")

    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = _flatten_columns(df, ticker)

    # Ensure consistent columns and always set 'Adj Close'
    # With auto_adjust=True, 'Close' is adjusted; create 'Adj Close' if missing.
    if "Adj Close" not in df.columns and "Close" in df.columns:
        df["Adj Close"] = df["Close"]

    # Forward-fill occasional gaps
    df = df.ffill()

    # Tag ticker
    df["Ticker"] = ticker.upper()
    return df
