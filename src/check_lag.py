# 시차 상관관계 분석 코드
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "dataset" / "final_dataset_2006_2021.csv"

def main():
    df = pd.read_csv(DATA_PATH)
    
    # 상관계수 확인을 위해 필요한 컬럼만 추출
    analysis_df = df[['news_sentiment', 'daily_return']].copy()
    
    print("📊 [심층 분석] 시차(Lag) 상관관계 분석")
    print("-" * 40)
    
    # 당일 상관관계
    corr_0 = analysis_df['news_sentiment'].corr(analysis_df['daily_return'])
    print(f"당일 반응 (Lag 0): {corr_0:.4f}")
    
    # 1일 뒤 반응 (어제 뉴스가 오늘 주가에?)
    # news_sentiment를 하루 shift해서 상관계수 계산
    corr_1 = analysis_df['news_sentiment'].shift(1).corr(analysis_df['daily_return'])
    print(f"1일 뒤 반응 (Lag 1): {corr_1:.4f}")
    
    # 2일 뒤 반응
    corr_2 = analysis_df['news_sentiment'].shift(2).corr(analysis_df['daily_return'])
    print(f"2일 뒤 반응 (Lag 2): {corr_2:.4f}")
    
    print("-" * 40)
    print("Tip: Lag 1의 상관계수가 Lag 0보다 높다면,")
    print("     '뉴스가 주가에 반영되기까지 하루 정도 시간이 걸린다'는 결론을 낼 수 있습니다.")

if __name__ == "__main__":
    main()