from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Any
import numpy as np
import pandas as pd
from .indicators import (
    sma, ema, rsi, macd, bollinger,
    daily_returns, cumulative_returns, rolling_volatility,
    drawdown_curve, max_drawdown
)

TRADING_DAYS = 252

@dataclass
class PerfSummary:
    start: str
    end: str
    days: int
    cagr: float
    total_return: float
    sharpe: float
    max_drawdown: float
    avg_daily_return: float
    std_daily_return: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["Adj Close"]
    out["SMA20"] = sma(close, 20)
    out["SMA50"] = sma(close, 50)
    out["EMA12"] = ema(close, 12)
    out["EMA26"] = ema(close, 26)
    out["RSI14"] = rsi(close, 14)
    macd_line, signal_line, hist = macd(close)
    out["MACD"] = macd_line
    out["MACD_SIGNAL"] = signal_line
    out["MACD_HIST"] = hist
    mid, upper, lower = bollinger(close)
    out["BB_MID"] = mid
    out["BB_UPPER"] = upper
    out["BB_LOWER"] = lower
    out["RET_DAILY"] = daily_returns(close)
    out["RET_CUM"] = cumulative_returns(out["RET_DAILY"].dropna())
    out["VOL21"] = rolling_volatility(out["RET_DAILY"], 21)
    out["DRAWDOWN"] = drawdown_curve(close)
    return out

def performance_summary(df: pd.DataFrame, risk_free_rate_annual: float = 0.0) -> PerfSummary:
    """
    Compute classic performance stats from Adj Close and daily returns.
    Sharpe uses (excess) daily return * sqrt(252).
    """
    if "Adj Close" not in df.columns:
        raise ValueError("DataFrame must include 'Adj Close' column.")
    ret = df["Adj Close"].pct_change().dropna()
    total_return = (df["Adj Close"].iloc[-1] / df["Adj Close"].iloc[0]) - 1.0
    n_days = (df.index[-1] - df.index[0]).days
    years = max(n_days / 365.25, 1e-9)
    cagr = (1 + total_return) ** (1 / years) - 1 if total_return > -1 else -1.0

    rf_daily = (1 + risk_free_rate_annual) ** (1 / TRADING_DAYS) - 1
    excess = ret - rf_daily
    sharpe = (excess.mean() / excess.std()) * np.sqrt(TRADING_DAYS) if excess.std() and len(excess) > 2 else float("nan")

    mdd = max_drawdown(df["Adj Close"])
    return PerfSummary(
        start=str(df.index[0].date()),
        end=str(df.index[-1].date()),
        days=n_days,
        cagr=float(cagr),
        total_return=float(total_return),
        sharpe=float(sharpe),
        max_drawdown=float(mdd),
        avg_daily_return=float(ret.mean()),
        std_daily_return=float(ret.std()),
    )
