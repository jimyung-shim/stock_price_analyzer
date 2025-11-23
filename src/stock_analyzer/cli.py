# src/stock_analyzer/cli.py
from __future__ import annotations

import argparse
from pathlib import Path

from .data import fetch_prices
from .report import write_report
from .news import fetch_news_counts_for_ticker


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="stock-analyzer",
        description=(
            "Download OHLCV data, run analysis, "
            "and crawl AWS-related news article counts (using Google News)."
        ),
    )

    # 티커: 하나만 분석 (예: AMZN)
    p.add_argument("ticker", help="Single ticker symbol, e.g. AMZN")

    # 기간 지정
    g = p.add_mutually_exclusive_group()
    g.add_argument(
        "--period",
        default="3y",
        help="yfinance period (e.g. 1y, 3y, 5y, max) [default: 3y]",
    )
    g.add_argument(
        "--range",
        nargs=2,
        metavar=("START", "END"),
        help="Explicit date range: YYYY-MM-DD YYYY-MM-DD",
    )

    p.add_argument("--interval", default="1d", help="Price data interval [default: 1d]")
    p.add_argument("-o", "--out", default="out", help="Output directory [default: out]")
    p.add_argument("--rf", type=float, default=0.0, help="Risk-free rate")

    # ---- 뉴스 관련 옵션 ----
    p.add_argument(
        "--news-query",
        default="Amazon Web Services",
        help="Search query for Google News [default: 'Amazon Web Services']",
    )
    p.add_argument("--news-start", help="News start date (YYYY-MM-DD)")
    p.add_argument("--news-end", help="News end date (YYYY-MM-DD)")
    p.add_argument("--news-dir", default="raw/news_data", help="News CSV output dir")
    
    # 구글 뉴스 크롤링은 차단 위험이 있으므로 끄고 켤 수 있게 옵션 추가
    p.add_argument(
        "--skip-news", 
        action="store_true", 
        help="Skip news crawling step (useful if you already have the data)"
    )

    return p


def main() -> None:
    args = build_parser().parse_args()

    # 1) 주가 데이터 가져오기
    if args.range:
        start, end = args.range
        df = fetch_prices(
            args.ticker,
            period=None,
            interval=args.interval,
            start=start,
            end=end,
        )
        label = f"{args.ticker.upper()}_{start}_to_{end}"
        price_start, price_end = start, end
    else:
        df = fetch_prices(
            args.ticker,
            period=args.period,
            interval=args.interval,
        )
        label = f"{args.ticker.upper()}_{args.period}_{args.interval}"
        price_start = df.index.min().strftime("%Y-%m-%d")
        price_end = df.index.max().strftime("%Y-%m-%d")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = write_report(
        df,
        out_dir,
        dataset_name=label,
        risk_free_rate_annual=args.rf,
    )

    # 2) 뉴스 크롤링 (옵션)
    if not args.skip_news:
        news_start = args.news_start or price_start
        news_end = args.news_end or price_end

        print("\n" + "="*40)
        print(f"🚀 Starting Google News Crawl")
        print(f"   Target Range: {news_start} ~ {news_end}")
        print("   Note: This may take a while. Press Ctrl+C to stop safely.")
        print("="*40 + "\n")

        news_df, news_path = fetch_news_counts_for_ticker(
            query=args.news_query,
            start=news_start,
            end=news_end,
            out_dir=args.news_dir,
        )
        
        print(f"   News CSV     : {news_path.resolve()}")
    else:
        print("⏭️  Skipping news crawling (--skip-news used)")

    # 3) 결과 요약
    print("\n✅ Analysis complete")
    print(f"   Ticker       : {args.ticker.upper()}")
    print(f"   Price output : {out_dir.resolve()}")
    print(f"   Report       : {report_path.resolve()}")


if __name__ == "__main__":
    main()