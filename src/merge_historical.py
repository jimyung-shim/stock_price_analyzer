# AWS 주가 데이터, CNBC 데이터 merge 하는 코드
import pandas as pd
from pathlib import Path

# ==========================================
# 1. 파일 경로 설정 (정확한 파일명 확인 필수!)
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent

# 가공된 뉴스 데이터 (이전 단계에서 생성함)
NEWS_PATH = BASE_DIR / "src" / "out" / "processed_news_sorted.csv"

# [중요] 사용자가 준비한 과거 주가 데이터 파일
STOCK_PATH = BASE_DIR / "raw" / "stock_data" / "Amazon stock data 2006.12-2021.10.csv"

# 최종 저장 경로
OUTPUT_PATH = BASE_DIR / "src" / "out" / "final_dataset_2006_2021.csv"

def main():
    print("🔄 과거 데이터(2006-2021) 병합 작업 시작...\n")

    # -------------------------------------------------------
    # 1. 뉴스 데이터 로드 및 일별 집계
    # -------------------------------------------------------
    if not NEWS_PATH.exists():
        print(f"❌ 뉴스 데이터 파일이 없습니다: {NEWS_PATH}")
        print("   먼저 'process_news.py'를 실행해주세요.")
        return

    print(f"📰 뉴스 데이터 로드 중... ({NEWS_PATH.name})")
    news_df = pd.read_csv(NEWS_PATH)
    
    # 날짜 형식 변환
    news_df['date'] = pd.to_datetime(news_df['date']).dt.date

    # [핵심] 기사 단위 데이터를 -> '일별(Daily)' 데이터로 변환
    # 같은 날짜의 기사들을 모아서 개수와 평균 감성을 구함
    daily_news = news_df.groupby('date').agg({
        'title': 'count',           # 기사 개수 (Volume)
        'sentiment': 'mean'         # 감성 점수 평균 (Sentiment)
    }).rename(columns={'title': 'news_count', 'sentiment': 'news_sentiment'})

    print(f"   -> 일별 뉴스 집계 완료: 총 {len(daily_news)}일치 데이터")

    # -------------------------------------------------------
    # 2. 주가 데이터 로드
    # -------------------------------------------------------
    if not STOCK_PATH.exists():
        print(f"❌ 주가 데이터 파일이 없습니다: {STOCK_PATH}")
        print(f"   경로를 확인해주세요: {STOCK_PATH}")
        return

    print(f"📈 주가 데이터 로드 중... ({STOCK_PATH.name})")
    try:
        # 천 단위 콤마(,)가 있는 경우 제거하면서 로드
        stock_df = pd.read_csv(STOCK_PATH, thousands=',')
    except Exception as e:
        print(f"❌ 주가 파일 읽기 에러: {e}")
        return

    # 컬럼명 공백 제거 및 문자열 변환
    stock_df.columns = [str(c).strip() for c in stock_df.columns]
    print(f"   ℹ️ 원본 주가 데이터 컬럼: {list(stock_df.columns)}") # 디버깅용 출력

    # 날짜 컬럼 찾기 ('Date', 'date', '날짜' 등 대응)
    date_col = None
    for col in stock_df.columns:
        if 'date' in col.lower():
            date_col = col
            break
    
    if not date_col:
        print("❌ 주가 데이터에서 'Date' 컬럼을 찾을 수 없습니다.")
        return

    # [수정됨] 날짜 변환 시 'utc=True' 옵션 추가하여 에러 해결
    try:
        stock_df['date'] = pd.to_datetime(stock_df[date_col], utc=True).dt.date
    except Exception as e:
        print(f"⚠️ 날짜 변환 중 오류 발생 (utc=True 시도): {e}")
        stock_df['date'] = pd.to_datetime(stock_df[date_col], errors='coerce', utc=True).dt.date
        
    stock_df.set_index('date', inplace=True)
    
    # [수정됨] 강력한 컬럼 이름 표준화 (Close/Last, 종가 등 모두 Close로 통일)
    rename_map = {}
    for col in stock_df.columns:
        c_lower = col.lower()
        if 'close' in c_lower and 'adj' not in c_lower: # 'Close', 'Close/Last' 등
            rename_map[col] = 'Close'
        elif 'adj' in c_lower and 'close' in c_lower:   # 'Adj Close'
            rename_map[col] = 'Adj Close'
        elif 'open' in c_lower:
            rename_map[col] = 'Open'
        elif 'high' in c_lower:
            rename_map[col] = 'High'
        elif 'low' in c_lower:
            rename_map[col] = 'Low'
        elif 'vol' in c_lower:
            rename_map[col] = 'Volume'
            
    if rename_map:
        stock_df.rename(columns=rename_map, inplace=True)
        print(f"   -> 컬럼명 표준화 결과: {list(stock_df.columns)}")

    # 'Close' 컬럼이 없으면 멈춤 (필수)
    if 'Close' not in stock_df.columns:
        print("❌ 오류: 'Close' (종가) 컬럼을 찾을 수 없습니다.")
        print("   현재 컬럼 목록을 확인하고 코드를 수정하거나 CSV 파일을 확인하세요.")
        return

    # -------------------------------------------------------
    # 3. 데이터 병합 (Left Join)
    # -------------------------------------------------------
    print("🔄 데이터 병합 중...")
    # 주가 데이터(Trade Days)를 기준으로 뉴스 데이터를 붙임
    merged_df = stock_df.join(daily_news, how='left')

    # -------------------------------------------------------
    # 4. 데이터 정제 (Cleaning)
    # -------------------------------------------------------
    # 결측치 처리 (뉴스가 없는 날)
    merged_df['news_count'] = merged_df['news_count'].fillna(0)
    merged_df['news_sentiment'] = merged_df['news_sentiment'].fillna(0) # 0은 중립

    # 주가 데이터(Open, High, Low, Close)가 문자열일 경우 숫자로 변환
    cols_to_numeric = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in cols_to_numeric:
        if col in merged_df.columns:
            # $ 표시 제거 등
            if merged_df[col].dtype == object:
                merged_df[col] = merged_df[col].astype(str).str.replace('$', '').str.replace(',', '')
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    # 변동성(Volatility) 계산: High - Low
    if 'High' in merged_df.columns and 'Low' in merged_df.columns:
        merged_df['volatility'] = merged_df['High'] - merged_df['Low']

    # 등락률(Daily Return) 계산
    if 'Close' in merged_df.columns:
        merged_df['daily_return'] = merged_df['Close'].pct_change()

    # [머신러닝 타겟 1] 내일 주가가 오를까? (1: 상승, 0: 하락/보합)
    merged_df['target_up_down'] = (merged_df['Close'].shift(-1) > merged_df['Close']).astype(int)

    # [머신러닝 타겟 2] 내일 변동성은 얼마일까?
    if 'volatility' in merged_df.columns:
        merged_df['target_volatility'] = merged_df['volatility'].shift(-1)

    # 마지막 날은 내일 데이터가 없으므로 제거
    merged_df.dropna(inplace=True)
    
    # 날짜 오름차순 정렬 (과거 -> 미래)
    merged_df.sort_index(ascending=True, inplace=True)

    # -------------------------------------------------------
    # 5. 저장
    # -------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(OUTPUT_PATH)
    
    print("\n" + "="*40)
    print("✅ 데이터 병합 완료! (2006-2021)")
    print(f"📂 저장 위치: {OUTPUT_PATH}")
    print(f"📊 데이터 크기: {merged_df.shape}")
    print("="*40)
    print(merged_df[['Close', 'news_count', 'news_sentiment', 'target_up_down']].head())

if __name__ == "__main__":
    main()