from __future__ import annotations
from typing import Optional
import pandas as pd
import yfinance as yf

def fetch_prices(
    ticker: str,
    period: Optional[str] = "5y",
    interval: str = "1d",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Download OHLCV from Yahoo Finance via yfinance.
    Returns a DataFrame with DateTimeIndex and columns:
    ['Open','High','Low','Close','Adj Close','Volume'].
    """
    if start or end:
        df = yf.download(ticker, start=start, end=end, interval=interval, auto_adjust=False, progress=False)
    else:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if df.empty:
        raise ValueError(f"No data returned for {ticker}. Check ticker or date range.")
    # Ensure index is tz-naive DatetimeIndex
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df.columns = [c.strip() for c in df.columns]
    # Forward-fill any missing prices (rare on splits/holidays)
    df = df.ffill()
    df["Ticker"] = ticker.upper()
    return df
