from __future__ import annotations
import argparse
from pathlib import Path
from .data import fetch_prices
from .report import write_report

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="stock-analyzer",
        description="Download stock prices and generate a Markdown report with indicators.",
    )
    p.add_argument("ticker", help="Ticker symbol (e.g., AAPL, TSLA, MSFT)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--period", default="5y", help="yfinance period (e.g., 1y, 5y, max) [default: 5y]")
    g.add_argument("--range", nargs=2, metavar=("START", "END"), help="YYYY-MM-DD YYYY-MM-DD")
    p.add_argument("--interval", default="1d", help="yfinance interval (e.g., 1d, 1wk, 1mo)")
    p.add_argument("-o", "--out", default="analysis_output", help="Output directory")
    p.add_argument("--rf", type=float, default=0.0, help="Annual risk-free rate as decimal (e.g., 0.03 for 3%)")
    return p

def main():
    args = build_parser().parse_args()
    if args.range:
        start, end = args.range
        df = fetch_prices(args.ticker, period=None, interval=args.interval, start=start, end=end)
        label = f"{args.ticker.upper()}_{start}_to_{end}"
    else:
        df = fetch_prices(args.ticker, period=args.period, interval=args.interval)
        label = f"{args.ticker.upper()}_{args.period}_{args.interval}"

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = write_report(df, out_dir, dataset_name=label, risk_free_rate_annual=args.rf)

    print("✅ Analysis complete")
    print(f"   Ticker : {args.ticker.upper()}")
    print(f"   Output : {out_dir.resolve()}")
    print(f"   Report : {report_path.resolve()}")

if __name__ == "__main__":
    main()
