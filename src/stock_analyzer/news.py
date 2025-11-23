# src/stock_analyzer/news.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterable, Tuple
import os
import time
from dotenv import load_dotenv

import requests
import pandas as pd

load_dotenv()

DATE_FMT = "%Y-%m-%d"


def _date_range(start: datetime, end: datetime) -> Iterable[datetime]:
    """start ~ end (inclusive) 하루 단위 반복자."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _fetch_daily_news_count(query: str, date: datetime, *, api_key: str) -> int:
    """
    특정 날짜에 대해 query에 해당하는 뉴스 기사 수를 가져온다.
    여기서는 NewsAPI.org의 /v2/everything 엔드포인트를 사용한 예시.
    """
    url = "https://newsapi.org/v2/everything"

    # 하루 단위 집계를 위해 from/to 를 같은 날짜+1일 로 설정
    from_str = date.strftime(DATE_FMT)
    to_str = (date + timedelta(days=1)).strftime(DATE_FMT)

    params = {
        "q": query,          # 예: "Amazon Web Services"
        "from": from_str,
        "to": to_str,
        "language": "en",
        "pageSize": 100,
        "sortBy": "publishedAt",
        "apiKey": api_key,
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # NewsAPI는 totalResults를 돌려줌
    total = int(data.get("totalResults", 0))
    return total


def fetch_news_counts_for_ticker(
    *,
    query: str,
    start: str,
    end: str,
    out_dir: str | Path = "raw/news_data",
    sleep_sec: float = 1.0,
) -> Tuple[pd.DataFrame, Path]:
    """
    [start, end] 구간 동안 하루 단위로 뉴스 기사 수를 카운트하고
    raw/news_data/ 아래에 CSV로 저장한다.

    Parameters
    ----------
    query : str
        검색 키워드 (예: "Amazon Web Services" 혹은 "AWS").
    start, end : str
        "YYYY-MM-DD" 형식의 시작/끝 날짜.
    out_dir : str | Path
        CSV 저장 디렉토리 (기본: raw/news_data).
    sleep_sec : float
        API rate limit을 피하기 위한 딜레이 (초).

    Returns
    -------
    df : pandas.DataFrame
        컬럼: [date, query, count]
    out_path : pathlib.Path
        저장된 CSV 파일 경로.
    """
    api_key = os.environ.get("NEWS_API_KEY")
    if not api_key:
        raise RuntimeError(
            "NEWS_API_KEY 환경 변수가 설정되어 있지 않습니다. "
            "NewsAPI.org API 키를 발급받아서 NEWS_API_KEY 로 export 해 주세요."
        )

    start_dt = datetime.strptime(start, DATE_FMT)
    end_dt = datetime.strptime(end, DATE_FMT)

    records = []
    for d in _date_range(start_dt, end_dt):
        try:
            count = _fetch_daily_news_count(query=query, date=d, api_key=api_key)
        except Exception as e:
            # 에러 발생 시 count를 None 으로 남겨두고 넘어간다.
            count = None
        records.append(
            {
                "date": d.strftime(DATE_FMT),
                "query": query,
                "count": count,
            }
        )
        if sleep_sec:
            time.sleep(sleep_sec)

    df = pd.DataFrame.from_records(records)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_query = query.replace(" ", "_").replace("/", "_")
    filename = f"{safe_query}_news_counts_{start}_to_{end}.csv"
    out_path = out_dir / filename
    df.to_csv(out_path, index=False)

    return df, out_path
