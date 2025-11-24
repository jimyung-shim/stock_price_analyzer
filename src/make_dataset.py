# src/make_dataset.py
import pandas as pd
import numpy as np
from textblob import TextBlob
from pathlib import Path
import os

# ========================================== 
# 1. 파일 경로 설정 (사용자 환경에 맞춰 수정 가능) 
# ========================================== 
BASE_DIR = Path(__file__).resolve().parent.parent # 프로젝트 루트 기준 
RAW_DIR = BASE_DIR / "raw" 
OUT_DIR = BASE_DIR / "src" / "out" 

# 사용자가 업로드한 파일명에 맞춰 경로 지정 
# (주의: 실제 파일명이 cbnc.txt 인지, cnbc_news_datase.csv 인지 확인 후 수정하세요) 
NEWS_FILE_PATH = RAW_DIR / "cbnc.txt" 
STOCK_FILE_PATH = RAW_DIR / "stock_data" / "Amazon stock data 2022.2-2025.2.csv" 
OUTPUT_FILE_PATH = OUT_DIR / "final_dataset_for_ml.csv" 

# ==========================================
# 2. 뉴스 데이터 처리 함수
# ==========================================
def process_news_data(file_path):
    print(f"📰 뉴스 데이터 로딩 중... : {file_path}")
    
    # 구분자(delimiter)가 콤마(,)인지 탭(\t)인지 파일 형태에 따라 다를 수 있음. 기본은 콤마.
    # on_bad_lines='skip': 형식이 깨진 라인은 무시 (Kaggle 데이터셋에 흔함)
    try:
        df = pd.read_csv(file_path, on_bad_lines='skip')
    except Exception as e:
        print(f"❌ 뉴스 파일 로드 실패: {e}")
        return None

    # 날짜 변환 (ISO 8601 format: 2021-09-29T17:09:39+0000)
    # errors='coerce': 변환 불가능한 날짜는 NaT로 처리
    if 'published_at' in df.columns:
        df['date'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True).dt.date
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    else:
        print("❌ 뉴스 데이터에 'published_at' 또는 'date' 컬럼이 없습니다.")
        return None

    # 날짜 없는 행 제거
    df = df.dropna(subset=['date'])

    # -------------------------------------------------------
    # 필터링: Amazon 관련 뉴스 + 거시 경제 뉴스 (프로젝트 범위 확장)
    # -------------------------------------------------------
    keywords = [
        'Amazon', 'AWS', 'AMZN', 'Bezos',  # 아마존 직접 관련
        'Tech', 'Cloud', 'Nasdaq',         # 섹터 관련
        'Fed', 'Inflation', 'Economy'      # 거시 경제 (주가에 큰 영향)
    ]
    
    # 대소문자 구분 없이 검색을 위한 정규표현식 생성
    pattern = '|'.join(keywords)
    
    # 제목(title)이나 설명(description)에 키워드가 포함된 기사만 추출
    mask = df['title'].str.contains(pattern, case=False, na=False)
    if 'description' in df.columns:
        mask |= df['description'].str.contains(pattern, case=False, na=False)
    
    filtered_df = df[mask].copy()
    print(f"   - 전체 {len(df)}개 중 관련 뉴스 {len(filtered_df)}개 추출 완료")

    # -------------------------------------------------------
    # 감성 분석 (Sentiment Analysis)
    # -------------------------------------------------------
    print("   - 감성 분석 수행 중 (시간이 조금 걸릴 수 있습니다)...")
    
    def calculate_sentiment(text):
        if not isinstance(text, str):
            return 0
        return TextBlob(text).sentiment.polarity

    # 제목과 요약문을 합쳐서 분석하면 더 정확함
    filtered_df['full_text'] = filtered_df['title'].astype(str) + " " + filtered_df['description'].fillna("").astype(str)
    filtered_df['sentiment'] = filtered_df['full_text'].apply(calculate_sentiment)

    # -------------------------------------------------------
    # 일별 집계 (Aggregation)
    # -------------------------------------------------------
    # 같은 날짜에 여러 기사가 있으므로 날짜별로 묶음
    daily_news = filtered_df.groupby('date').agg({
        'title': 'count',           # 기사 개수 (Volume)
        'sentiment': 'mean'         # 평균 감성 점수 (Sentiment)
    }).rename(columns={'title': 'news_count', 'sentiment': 'news_sentiment'})

    return daily_news

# ==========================================
# 3. 주가 데이터 처리 함수
# ==========================================
def process_stock_data(file_path):
    print(f"📈 주가 데이터 로딩 중... : {file_path}")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ 주가 파일 로드 실패: {e}")
        return None

    # 날짜 컬럼 찾기 (대소문자 처리)
    col_map = {c: c.lower() for c in df.columns}
    date_col = None
    for c in df.columns:
        if 'date' in c.lower():
            date_col = c
            break
    
    if not date_col:
        print("❌ 주가 데이터에 Date 컬럼을 찾을 수 없습니다.")
        return None

    df['date'] = pd.to_datetime(df[date_col]).dt.date
    df.set_index('date', inplace=True)
    
    return df

# ==========================================
# 4. 메인 실행 함수
# ==========================================
def main():
    # 1. 데이터 로드 및 가공
    news_df = process_news_data(NEWS_FILE_PATH)
    stock_df = process_stock_data(STOCK_FILE_PATH)

    if news_df is None or stock_df is None:
        print("❌ 데이터 처리를 중단합니다.")
        return

    # 2. 데이터 병합 (Left Join: 주가 데이터 기준)
    # 주식 시장이 열린 날을 기준으로 뉴스를 붙임
    print("🔄 데이터 병합 중...")
    merged_df = stock_df.join(news_df, how='left')

    # 3. 결측치 처리
    # 뉴스가 없는 날은 기사수=0, 감성점수=0(중립)으로 채움
    merged_df['news_count'] = merged_df['news_count'].fillna(0)
    merged_df['news_sentiment'] = merged_df['news_sentiment'].fillna(0)

    # 4. 파생 변수 생성 (머신러닝용)
    # 변동성 (고가 - 저가)
    if 'High' in merged_df.columns and 'Low' in merged_df.columns:
        merged_df['volatility'] = merged_df['High'] - merged_df['Low']
    
    # 전일 대비 등락률 (Return)
    if 'Close' in merged_df.columns:
        merged_df['daily_return'] = merged_df['Close'].pct_change()

    # [중요] 타겟 변수 생성 (내일의 주가 변동을 예측하기 위해)
    # shift(-1)을 해서 '다음날의 변동성'을 현재 행에 가져옴
    merged_df['target_volatility'] = merged_df['volatility'].shift(-1)
    
    # 5. 최종 저장
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(OUTPUT_FILE_PATH)
    
    print("\n" + "="*40)
    print("✅ 최종 데이터셋 생성 완료!")
    print(f"📂 저장 위치: {OUTPUT_FILE_PATH}")
    print("="*40)
    print(merged_df[['Close', 'news_count', 'news_sentiment', 'volatility']].head(10))
    print("="*40)

if __name__ == "__main__":
    main()