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
            "and crawl AWS-related news article counts."
        ),
    )

    # 티커: 하나만 분석 (예: AMZN)
    p.add_argument("ticker", help="Single ticker symbol, e.g. AMZN")

    # 기간 지정: period 또는 range 중 하나
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
        help="Explicit date range: YYYY-MM-DD YYYY-MM-DD "
             "(예: 2022-02-01 2025-02-19)",
    )

    p.add_argument(
        "--interval",
        default="1d",
        help="Price data interval: 1d, 1wk, 1mo [default: 1d]",
    )

    p.add_argument(
        "-o", "--out",
        default="out",
        help="Output directory for price analysis artifacts [default: out]",
    )

    p.add_argument(
        "--rf",
        type=float,
        default=0.0,
        help="Annual risk-free rate (decimal), used in performance summary.",
    )

    # ---- 뉴스 관련 옵션 ----
    p.add_argument(
        "--news-query",
        default="Amazon Web Services",
        help=(
            "뉴스 기사 수를 셀 때 사용할 검색 키워드 "
            "[기본: 'Amazon Web Services']"
        ),
    )
    p.add_argument(
        "--news-start",
        help="뉴스 데이터 시작일 (YYYY-MM-DD). 지정하지 않으면 주가 시작일을 사용.",
    )
    p.add_argument(
        "--news-end",
        help="뉴스 데이터 종료일 (YYYY-MM-DD). 지정하지 않으면 주가 종료일을 사용.",
    )
    p.add_argument(
        "--news-dir",
        default="raw/news_data",
        help="뉴스 기사 수 CSV를 저장할 디렉토리 [기본: raw/news_data]",
    )

    return p


def main() -> None:
    args = build_parser().parse_args()

    # 1) 주가 데이터 가져오기 (기존 기능 유지)
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
        # DataFrame 인덱스에서 날짜 범위 자동 추론
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

    # 2) AWS 관련 뉴스 기사 수 크롤링 (신규 기능)
    news_start = args.news_start or price_start
    news_end = args.news_end or price_end

    # 프로젝트 요구사항: 2022.2 ~ 2025.2 범위에 대해 수집하고 싶다면,
    # 실행 시 이렇게 부르면 됨:
    #   stock-analyzer AMZN --range 2022-02-01 2025-02-19
    # 또는 --news-start/--news-end 로 별도 지정 가능.

    news_df, news_path = fetch_news_counts_for_ticker(
        query=args.news_query,
        start=news_start,
        end=news_end,
        out_dir=args.news_dir,
    )

    # 3) 결과 요약 출력
    print("✅ Analysis complete")
    print(f"   Ticker       : {args.ticker.upper()}")
    print(f"   Price output : {out_dir.resolve()}")
    print(f"   Report       : {report_path.resolve()}")
    print("")
    print("📈 News crawling")
    print(f"   News query   : {args.news_query!r}")
    print(f"   News range   : {news_start} → {news_end}")
    print(f"   News CSV     : {news_path.resolve()}")


if __name__ == "__main__":
    main()
