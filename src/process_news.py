# src/process_news.py
import pandas as pd
from pathlib import Path
from textblob import TextBlob # 감성 분석용

# ==========================================
# 1. 파일 경로 설정
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent # 프로젝트 루트
RAW_NEWS_PATH = BASE_DIR / "raw" / "cnbc_news_datase.csv"
OUTPUT_PATH = BASE_DIR / "src" / "out" / "processed_news_sorted.csv"

def calculate_sentiment(text):
    """텍스트의 감성 점수(-1.0 ~ 1.0)를 계산합니다."""
    if not isinstance(text, str):
        return 0
    return TextBlob(text).sentiment.polarity

def main():
    print(f"📰 뉴스 데이터 로딩 중... : {RAW_NEWS_PATH}")
    
    # 1. 데이터 로드
    # on_bad_lines='skip': 형식이 잘못된 라인은 건너뜀
    try:
        df = pd.read_csv(RAW_NEWS_PATH, on_bad_lines='skip')
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}")
        return

    print(f"   - 원본 데이터 개수: {len(df)}개")

    # 2. 날짜 컬럼 변환 및 정렬 준비
    # 'published_at' 컬럼을 datetime 객체로 변환 (UTC 기준)
    if 'published_at' in df.columns:
        df['datetime'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
        # 시간 정보 제거하고 날짜만 남김 (분석 단위가 '일' 이므로)
        df['date'] = df['datetime'].dt.date
    else:
        print("❌ 'published_at' 컬럼이 없습니다.")
        return

    # 날짜 변환 실패한 행(NaT) 제거
    df = df.dropna(subset=['date'])

    # 3. 관련 뉴스 필터링 (Amazon, AWS, 경제 이슈 등)
    keywords = [
        'Amazon', 'AWS', 'AMZN', 'Bezos', 
        'Tech', 'Cloud', 'Nasdaq', 
        'Fed', 'Economy', 'Inflation', 'Recession'
    ]
    pattern = '|'.join(keywords)
    
    # 제목이나 본문에 키워드가 있는 경우만 추출
    mask = df['title'].str.contains(pattern, case=False, na=False) | \
           df['description'].str.contains(pattern, case=False, na=False)
    
    filtered_df = df[mask].copy()
    print(f"   - 키워드 필터링 후: {len(filtered_df)}개")

    # 4. 날짜 오름차순 정렬 (과거 -> 현재)
    sorted_df = filtered_df.sort_values(by='datetime', ascending=True)

    # 5. 인덱스 재설정 (0부터 다시 번호 매기기)
    sorted_df = sorted_df.reset_index(drop=True)

    # 6. 감성 분석 수행 (미리 해두면 나중에 편함)
    print("   - 감성 분석(Sentiment Analysis) 계산 중...")
    # 제목 + 설명 합쳐서 분석
    sorted_df['full_text'] = sorted_df['title'].astype(str) + " " + sorted_df['description'].fillna("").astype(str)
    sorted_df['sentiment'] = sorted_df['full_text'].apply(calculate_sentiment)

    # 7. 필요한 컬럼만 선택해서 저장
    cols_to_keep = ['date', 'title', 'sentiment', 'url', 'short_description'] 
    # 원본에 short_description이 있다면 포함, 없으면 description 사용 등 유동적으로
    if 'description' in sorted_df.columns:
        cols_to_keep = ['date', 'title', 'sentiment', 'description']
    
    final_df = sorted_df[cols_to_keep]

    # 결과 저장
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)

    print("\n" + "="*40)
    print("✅ 뉴스 데이터 가공 및 정렬 완료!")
    print(f"📂 저장 위치: {OUTPUT_PATH}")
    print("="*40)
    print(final_df.head())

if __name__ == "__main__":
    main()