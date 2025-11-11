from __future__ import annotations
import argparse
from typing import List, Dict, Any
import pandas as pd
from .data import fetch_prices
from .analysis import compute_indicators, performance_summary
from .export import save_artifacts

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="stock-parse",
        description="Parse OHLCV, compute indicators & metrics, and write service-ready files."
    )
    p.add_argument("--tickers", required=True,
                   help="Comma-separated tickers, e.g., AAPL,MSFT,NVDA")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--period", default="5y",
                   help="yfinance period (e.g., 1y, 3y, 5y, 10y, max) [default: 5y]")
    g.add_argument("--range", nargs=2, metavar=("START","END"),
                   help="YYYY-MM-DD YYYY-MM-DD")
    p.add_argument("--interval", default="1d", help="1d, 1wk, 1mo")
    p.add_argument("-o", "--out", default="out", help="Output directory (default: out)")
    p.add_argument("--rf", type=float, default=0.0, help="Annual risk-free rate (decimal)")
    p.add_argument("--format", choices=["parquet","csv"], default="parquet",
                   help="Output table format (default: parquet)")
    return p

def _tidy_prices(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure Date index and Ticker column are present (data.fetch_prices already sets Ticker)
    df = df.copy()
    df.index.name = "date"
    cols = ["Open","High","Low","Close","Adj Close","Volume","Ticker"]
    df = df[cols]
    return df

def _tidy_indicators(df_ind: pd.DataFrame, ticker: str) -> pd.DataFrame:
    keep = ["SMA20","SMA50","EMA12","EMA26","RSI14","MACD","MACD_SIGNAL","MACD_HIST",
            "BB_MID","BB_UPPER","BB_LOWER","VOL21","DRAWDOWN"]
    out = df_ind[keep].copy()
    out.index.name = "date"
    out["Ticker"] = ticker
    return out

def _tidy_returns(df_ind: pd.DataFrame, ticker: str) -> pd.DataFrame:
    keep = []
    if "RET_DAILY" in df_ind: keep.append("RET_DAILY")
    if "RET_CUM" in df_ind:   keep.append("RET_CUM")
    out = df_ind[keep].copy()
    out.index.name = "date"
    out["Ticker"] = ticker
    return out

def main():
    args = build_parser().parse_args()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    prices_all = []
    indicators_all = []
    returns_all = []
    perf_rows: List[Dict[str, Any]] = []

    for t in tickers:
        if args.range:
            start, end = args.range
            raw = fetch_prices(t, period=None, interval=args.interval, start=start, end=end)
        else:
            raw = fetch_prices(t, period=args.period, interval=args.interval)

        ind = compute_indicators(raw)
        perf = performance_summary(raw, risk_free_rate_annual=args.rf)

        prices_all.append(_tidy_prices(raw).assign(Ticker=t))
        indicators_all.append(_tidy_indicators(ind, t))
        returns_all.append(_tidy_returns(ind, t))
        perf_rows.append({"ticker": t, **perf.to_dict()})

    prices = pd.concat(prices_all).sort_index()
    indicators = pd.concat(indicators_all).sort_index()
    returns = pd.concat(returns_all).sort_index()

    save_artifacts(
        prices=prices,
        indicators=indicators,
        returns=returns,
        perf_rows=perf_rows,
        out_dir=args.out,
        fmt=args.format,
    )

    print("✅ Parse & analysis complete")
    print(f"   Tickers : {', '.join(tickers)}")
    print(f"   Output  : {args.out}")

if __name__ == "__main__":
    main()
