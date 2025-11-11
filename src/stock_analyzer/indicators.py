from __future__ import annotations
import numpy as np
import pandas as pd

def sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window).mean()

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    gain = up.ewm(alpha=1/period, adjust=False).mean()
    loss = down.ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out

def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger(close: pd.Series, window: int = 20, num_std: float = 2.0):
    mid = sma(close, window)
    std = close.rolling(window).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return mid, upper, lower

def daily_returns(adj_close: pd.Series) -> pd.Series:
    return adj_close.pct_change().dropna()

def cumulative_returns(returns: pd.Series) -> pd.Series:
    return (1 + returns).cumprod() - 1

def rolling_volatility(returns: pd.Series, window: int = 21) -> pd.Series:
    # Approx. monthly (21 trading days) volatility (annualize by sqrt(252) later if needed)
    return returns.rolling(window).std()

def drawdown_curve(adj_close: pd.Series) -> pd.Series:
    cummax = adj_close.cummax()
    dd = adj_close / cummax - 1.0
    return dd

def max_drawdown(adj_close: pd.Series) -> float:
    return float(drawdown_curve(adj_close).min())
