# src/stock_analyzer/news.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterable, Tuple
import time
import random
import pandas as pd
from GoogleNews import GoogleNews

# 날짜 포맷
# yfinance 등 내부 데이터용: YYYY-MM-DD
DATE_FMT_ISO = "%Y-%m-%d"
# GoogleNews 라이브러리 요청용: MM/DD/YYYY
DATE_FMT_US = "%m/%d/%Y"

def _date_range(start: datetime, end: datetime) -> Iterable[datetime]:
    """start ~ end (inclusive) 하루 단위 반복자."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def _fetch_daily_google_news_count(
    googlenews: GoogleNews,
    query: str,
    date: datetime
) -> int:
    """
    GoogleNews 라이브러리를 사용해 특정 날짜의 기사 수를 가져옵니다.
    (검색 결과 리스트의 길이를 반환)
    """
    date_str_us = date.strftime(DATE_FMT_US) # MM/DD/YYYY
    
    # 검색 기간 설정 (하루)
    googlenews.set_time_range(date_str_us, date_str_us)
    
    # 검색 실행
    googlenews.search(query)
    
    # 결과 가져오기
    # result()는 기본적으로 첫 페이지의 결과 리스트를 반환합니다.
    # 정확한 전체 기사 수(Total count)는 구글이 UI에서 숨기는 경우가 많아,
    # 여기서는 "검색된 주요 기사 리스트의 개수"를 화제성 지표로 사용합니다.
    results = googlenews.result()
    count = len(results)
    
    # 다음 검색을 위해 결과 초기화 (필수)
    googlenews.clear()
    
    return count

def fetch_news_counts_for_ticker(
    *,
    query: str,
    start: str,
    end: str,
    out_dir: str | Path = "raw/news_data",
    sleep_min: float = 2.0,
    sleep_max: float = 5.0,
) -> Tuple[pd.DataFrame, Path]:
    """
    [start, end] 구간 동안 하루 단위로 Google News를 크롤링하여
    기사 수를 카운트하고 CSV로 저장한다.

    Parameters
    ----------
    query : str
        검색 키워드 (예: "Amazon Web Services").
    start, end : str
        "YYYY-MM-DD" 형식의 시작/끝 날짜.
    out_dir : str | Path
        CSV 저장 디렉토리.
    sleep_min, sleep_max : float
        구글 차단 방지를 위한 랜덤 대기 시간 범위 (초).

    Returns
    -------
    df : pandas.DataFrame
        컬럼: [date, query, count]
    out_path : pathlib.Path
        저장된 CSV 파일 경로.
    """
    
    # GoogleNews 객체 초기화 (언어: 영어, 지역: 미국)
    googlenews = GoogleNews(lang='en', region='US')
    # 인코딩 설정 (가끔 깨지는 문제 방지)
    googlenews.set_encode('utf-8')

    start_dt = datetime.strptime(start, DATE_FMT_ISO)
    end_dt = datetime.strptime(end, DATE_FMT_ISO)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # 파일명 미리 생성
    safe_query = query.replace(" ", "_").replace("/", "_")
    filename = f"{safe_query}_news_counts_{start}_to_{end}.csv"
    out_path = out_dir / filename

    # 이미 파일이 있다면 로드해서 중단된 지점부터 이어하기 (Resumable)
    if out_path.exists():
        print(f"Found existing file: {out_path}. Resuming...")
        df_exist = pd.read_csv(out_path)
        records = df_exist.to_dict("records")
        # 마지막 날짜 확인
        if not df_exist.empty:
            last_date_str = df_exist.iloc[-1]["date"]
            last_date = datetime.strptime(last_date_str, DATE_FMT_ISO)
            # 시작일을 마지막 기록 다음 날로 조정
            start_dt = last_date + timedelta(days=1)
    else:
        records = []

    print(f"🔍 Starting crawl for '{query}' from {start_dt.date()} to {end_dt.date()}")
    
    try:
        for d in _date_range(start_dt, end_dt):
            d_str = d.strftime(DATE_FMT_ISO)
            
            try:
                count = _fetch_daily_google_news_count(googlenews, query, d)
            except Exception as e:
                print(f"⚠️ Error on {d_str}: {e}")
                count = 0 # 에러 시 0으로 처리하고 진행
                
                # 에러 발생 시 조금 더 길게 대기
                time.sleep(10) 

            print(f"   [{d_str}] found: {count} articles")
            
            records.append({
                "date": d_str,
                "query": query,
                "count": count,
            })

            # 중간 저장 (데이터 유실 방지)
            if len(records) % 10 == 0:
                pd.DataFrame(records).to_csv(out_path, index=False)

            # 차단 방지를 위한 랜덤 슬립
            sleep_time = random.uniform(sleep_min, sleep_max)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Crawling interrupted by user. Saving progress...")
    
    # 최종 저장
    df = pd.DataFrame(records)
    df.to_csv(out_path, index=False)
    
    print(f"✅ Saved news data to: {out_path}")
    return df, out_path