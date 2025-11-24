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
    특정 날짜의 기사 수를 가져오기 위해 페이지를 넘기며 수집합니다.
    시간 절약 및 차단 방지를 위해 최대 5페이지(약 50개)까지만 확인합니다.
    """
    date_str_us = date.strftime(DATE_FMT_US) # MM/DD/YYYY
    
    # 기간 설정 및 초기 검색
    googlenews.set_time_range(date_str_us, date_str_us)
    googlenews.search(query)
    
    # 첫 페이지 결과 수 확인
    results = googlenews.result()
    count = len(results)
    
    # 첫 페이지가 10개 미만이면 더 볼 필요 없음 (그게 전체 개수임)
    if count < 10:
        googlenews.clear()
        return count

    # 기사가 많을 경우 2~5페이지까지 추가 탐색 (최대 50개까지 카운트)
    # 3년치 데이터를 수집해야 하므로, 속도를 위해 5페이지로 제한하는 것이 현실적입니다.
    max_pages = 5 
    
    for page in range(2, max_pages + 1):
        try:
            googlenews.get_page(page)
            new_results = googlenews.result()
            new_count = len(new_results)
            
            # 더 이상 기사가 늘어나지 않으면(마지막 페이지 도달) 중단
            if new_count == count:
                break
            
            count = new_count
            
            # 페이지 넘길 때마다 아주 짧은 대기 (기계적 접근 방지)
            time.sleep(0.5)
            
        except Exception:
            # 페이지 로드 실패 시 현재까지 카운트 반환
            break
            
    # 다음 날짜를 위해 결과 초기화 (필수)
    googlenews.clear()
    return count

def fetch_news_counts_for_ticker(
    *,
    query: str,
    start: str,
    end: str,
    out_dir: str | Path = "raw/news_data",
    sleep_min: float = 1.5,
    sleep_max: float = 3.0,
) -> Tuple[pd.DataFrame, Path]:
    """
    Google News를 크롤링하여 일별 기사 수(Trend)를 저장합니다.
    """
    
    # GoogleNews 객체 초기화
    googlenews = GoogleNews(lang='en', region='US')
    googlenews.set_encode('utf-8')

    start_dt = datetime.strptime(start, DATE_FMT_ISO)
    end_dt = datetime.strptime(end, DATE_FMT_ISO)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    safe_query = query.replace(" ", "_").replace("/", "_")
    filename = f"{safe_query}_news_counts_{start}_to_{end}.csv"
    out_path = out_dir / filename

    # 이어하기(Resume) 기능
    records = []
    if out_path.exists():
        try:
            print(f"📂 Found existing file: {out_path}. Checking last date...")
            df_exist = pd.read_csv(out_path)
            if not df_exist.empty:
                last_date_str = df_exist.iloc[-1]["date"]
                last_date = datetime.strptime(last_date_str, DATE_FMT_ISO)
                if last_date >= start_dt:
                    start_dt = last_date + timedelta(days=1)
                    records = df_exist.to_dict("records")
                    print(f"⏭️  Resuming from {start_dt.date()}...")
        except Exception as e:
            print(f"⚠️ Error reading existing file: {e}. Starting fresh.")
            records = []

    if start_dt > end_dt:
        print("✅ All data already collected.")
        return pd.DataFrame(records), out_path

    print(f"🔍 Starting deep crawl for '{query}' from {start_dt.date()} to {end_dt.date()}")
    print("   (Checking up to 5 pages per day to capture trends...)")
    
    try:
        for d in _date_range(start_dt, end_dt):
            d_str = d.strftime(DATE_FMT_ISO)
            
            try:
                count = _fetch_daily_google_news_count(googlenews, query, d)
            except Exception as e:
                print(f"⚠️ Error on {d_str}: {e}")
                count = 0 
                time.sleep(5) # 에러 시 잠시 대기

            print(f"   [{d_str}] found: {count} articles")
            
            records.append({
                "date": d_str,
                "query": query,
                "count": count,
            })

            # 데이터 유실 방지를 위해 5일마다 저장
            if len(records) % 5 == 0:
                pd.DataFrame(records).to_csv(out_path, index=False)

            # 차단 방지 대기
            time.sleep(random.uniform(sleep_min, sleep_max))

    except KeyboardInterrupt:
        print("\n🛑 Crawling interrupted by user. Saving progress...")
    
    # 최종 저장
    df = pd.DataFrame(records)
    df.to_csv(out_path, index=False)
    
    print(f"✅ Saved news data to: {out_path}")
    return df, out_path