# src/merge_dataset.py
import pandas as pd
from pathlib import Path

# ==========================================
# 1. 파일 경로 설정
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent
# 앞 단계에서 만든 뉴스 파일
NEWS_PATH = BASE_DIR / "src" / "out" / "processed_news_sorted.csv"
# 앞 단계에서 stock-analyzer가 만든 지표 포함 주가 파일
STOCK_PATH = BASE_DIR / "src" / "out" / "timeseries_with_indicators.csv"
# 최종 저장 경로
OUTPUT_PATH = BASE_DIR / "src" / "out" / "final_dataset_for_ml.csv"

def main():
    print("🔄 데이터 병합 작업 시작...")

    # -------------------------------------------------------
    # 1. 뉴스 데이터 로드 및 일별 집계 (Aggregation)
    # -------------------------------------------------------
    if not NEWS_PATH.exists():
        print(f"❌ 뉴스 데이터 파일이 없습니다: {NEWS_PATH}")
        return

    print(f"   - 뉴스 데이터 로드 중: {NEWS_PATH.name}")
    news_df = pd.read_csv(NEWS_PATH)
    
    # 'date' 컬럼을 datetime 형식으로 변환
    news_df['date'] = pd.to_datetime(news_df['date']).dt.date

    # [핵심] 일별로 그룹화하여 '기사 수'와 '평균 감성 점수' 계산
    daily_news = news_df.groupby('date').agg({
        'title': 'count',           # 기사 개수
        'sentiment': 'mean'         # 감성 점수 평균
    }).rename(columns={'title': 'news_count', 'sentiment': 'news_sentiment'})

    print(f"   - 일별 뉴스 집계 완료: 총 {len(daily_news)}일치 데이터")

    # -------------------------------------------------------
    # 2. 주가 데이터 로드
    # -------------------------------------------------------
    if not STOCK_PATH.exists():
        print(f"❌ 주가 데이터 파일이 없습니다: {STOCK_PATH}")
        return

    print(f"   - 주가 데이터 로드 중: {STOCK_PATH.name}")
    stock_df = pd.read_csv(STOCK_PATH)
    
    # 주가 데이터의 날짜 컬럼 찾기 (보통 'date' 또는 'Date')
    date_col = 'date' if 'date' in stock_df.columns else 'Date'
    stock_df[date_col] = pd.to_datetime(stock_df[date_col]).dt.date
    
    # 날짜를 인덱스로 설정 (병합을 위해)
    stock_df.set_index(date_col, inplace=True)

    # -------------------------------------------------------
    # 3. 데이터 병합 (Left Join)
    # -------------------------------------------------------
    # 주가 데이터(왼쪽) 기준으로 뉴스 데이터(오른쪽)를 합침
    # 주식 시장이 열린 날짜만 남기기 위함
    merged_df = stock_df.join(daily_news, how='left')

    # -------------------------------------------------------
    # 4. 결측치(NaN) 처리
    # -------------------------------------------------------
    # 뉴스가 없는 날은 기사 수 0, 감성 점수 0(중립)으로 채움
    merged_df['news_count'] = merged_df['news_count'].fillna(0)
    merged_df['news_sentiment'] = merged_df['news_sentiment'].fillna(0)

    # -------------------------------------------------------
    # 5. 머신러닝용 타겟(Target) 변수 생성
    # -------------------------------------------------------
    # 우리가 예측하고 싶은 것: "내일 주가가 얼마나 변동할까?"
    # shift(-1)을 사용하여 '다음 날'의 데이터를 '오늘' 행에 가져옴
    
    # Target 1: 다음 날의 변동성 (High - Low)
    # (이미 VOL21 등의 지표가 있지만, 직관적인 일일 변동폭을 타겟으로 설정)
    if 'High' in merged_df.columns and 'Low' in merged_df.columns:
        today_volatility = merged_df['High'] - merged_df['Low']
        merged_df['target_volatility'] = today_volatility.shift(-1)

    # Target 2: 다음 날의 등락 여부 (1: 상승, 0: 하락)
    if 'Close' in merged_df.columns:
        # 다음 날 종가 > 오늘 종가
        merged_df['target_up_down'] = (merged_df['Close'].shift(-1) > merged_df['Close']).astype(int)

    # 마지막 행은 '내일' 데이터가 없으므로 결측치가 생김 -> 제거
    merged_df = merged_df.dropna()

    # -------------------------------------------------------
    # 6. 저장
    # -------------------------------------------------------
    merged_df.to_csv(OUTPUT_PATH)
    
    print("\n" + "="*40)
    print("✅ 데이터 병합 완료! (머신러닝 준비 끝)")
    print(f"📂 저장 파일: {OUTPUT_PATH}")
    print(f"📊 데이터 크기: {merged_df.shape}")
    print("="*40)
    print("미리보기 (처음 5줄):")
    print(merged_df[['Close', 'news_count', 'news_sentiment', 'target_volatility']].head())

if __name__ == "__main__":
    main()
