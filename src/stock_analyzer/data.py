# src/stock_analyzer/data.py
from __future__ import annotations
from typing import Optional
import pandas as pd
import yfinance as yf

WANTED_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

def _flatten_columns(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    # 1. MultiIndex 처리: 티커 레벨 제거 시도
    if isinstance(df.columns, pd.MultiIndex):
        # Prefer inner level (fields) for single ticker
        try:
            # 레벨이 2개 이상일 때, ticker가 포함된 레벨을 찾아 제거
            df.columns = df.columns.droplevel(1)
        except Exception:
            pass

        # 여전히 MultiIndex라면 그냥 문자열로 합침
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join(map(str, c)).strip() for c in df.columns]

    # 2. 문자열 표준화 (Title Case 변환)
    # 예: 'close' -> 'Close', 'Close AMZN' -> 'Close Amzn'
    df.columns = [str(c).strip().title() for c in df.columns]

    # 3. 핵심 컬럼 매핑
    # 컬럼명에 'Close', 'Open' 등이 포함되어 있으면 해당 이름으로 변경
    new_cols = {}
    current_cols = list(df.columns)
    
    # WANTED_COLS: ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    # "Adj Close"가 "Close"보다 먼저 매핑되도록 길이 역순 정렬 등의 주의가 필요하지만,
    # 여기서는 명시적으로 체크.
    
    for col in current_cols:
        if "Adj Close" in col:
            new_cols[col] = "Adj Close"
        elif "Close" in col:
            new_cols[col] = "Close"
        elif "Open" in col:
            new_cols[col] = "Open"
        elif "High" in col:
            new_cols[col] = "High"
        elif "Low" in col:
            new_cols[col] = "Low"
        elif "Volume" in col:
            new_cols[col] = "Volume"

    if new_cols:
        df = df.rename(columns=new_cols)

    # 4. 중복 컬럼 제거 (혹시라도 중복이 생겼을 경우)
    df = df.loc[:, ~df.columns.duplicated()]

    # 5. 최종 필터링
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

    print(f"DEBUG: Columns form yfinance: {df.columns}")

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
