# 데이터 분석 코드
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

# ==========================================
# 0. 파일 경로 설정 (방금 만든 데이터셋 경로)
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent
# 파일명이 정확한지 꼭 확인하세요!
DATA_PATH = BASE_DIR / "dataset" / "final_dataset_2006_2021.csv"
IMG_OUT_DIR = BASE_DIR / "src" / "out" / "graphs"

# 그래프 저장 폴더 생성
IMG_OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("📊 [Amazon 2006-2021] 프로젝트 데이터 분석 시작...\n")
    
    # 1. 데이터 로드
    if not DATA_PATH.exists():
        print(f"❌ 데이터 파일이 없습니다: {DATA_PATH}")
        return
        
    df = pd.read_csv(DATA_PATH)
    
    # 날짜 처리
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
    
    # 데이터 건전성 체크
    print(f"   - 전체 데이터 개수: {len(df)}일")
    news_exists_days = df[df['news_count'] > 0].shape[0]
    print(f"   - 뉴스가 있는 날: {news_exists_days}일 (전체의 {news_exists_days/len(df)*100:.1f}%)")

    # -----------------------------------------------------------
    # [과제 필수 1] groupby를 사용한 통계 분석
    # -----------------------------------------------------------
    print("\n✅ [1/3] 통계 분석 (Groupby) 수행 중...")
    
    # 연도별(Year) 그룹화
    df['Year'] = df.index.year
    
    # 3가지 메소드 사용: sum, mean, max
    yearly_stats = df.groupby('Year').agg({
        'news_count': 'sum',          # 연간 총 뉴스 기사 수
        'news_sentiment': 'mean',     # 연간 평균 뉴스 감성
        'Close': 'mean',              # 연간 평균 주가
        'volatility': 'mean'          # 연간 평균 변동성
    })
    
    print("\n--- 연도별 통계 요약 (최근 5년) ---")
    print(yearly_stats.tail())
    
    # CSV로 저장 (리포트용)
    yearly_stats.to_csv(BASE_DIR / "src" / "out" / "yearly_statistics.csv")

    # -----------------------------------------------------------
    # [과제 필수 2] 그래프 그리기 (2종 이상)
    # -----------------------------------------------------------
    print("\n✅ [2/3] 시각화 (Visualization) 수행 중...")
    
    sns.set(style="whitegrid")
    
    # --- 그래프 1: 연도별 뉴스 기사 수 변화 (Bar Plot) ---
    plt.figure(figsize=(10, 6))
    sns.barplot(x=yearly_stats.index, y=yearly_stats['news_count'], color='skyblue')
    plt.title('Annual News Volume Trend (2006-2021)')
    plt.ylabel('Total News Articles')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(IMG_OUT_DIR / "graph1_news_volume_trend.png")
    plt.close()
    
    # --- 그래프 2: 주가와 감성 점수의 관계 (Scatter Plot) ---
    # 데이터가 너무 많으므로 뉴스가 있는 날만 필터링해서 그림
    plot_df = df[df['news_count'] > 0]
    
    plt.figure(figsize=(10, 6))
    # 감성 점수에 따라 색상 다르게 (양수: 빨강, 음수: 파랑)
    sns.scatterplot(data=plot_df, x='news_sentiment', y='daily_return', 
                    hue=plot_df['news_sentiment'] > 0, palette={True: 'red', False: 'blue'}, alpha=0.6)
    plt.title('News Sentiment vs Daily Return')
    plt.xlabel('Sentiment Score (-1 to 1)')
    plt.ylabel('Daily Return')
    plt.legend(title='Positive Sentiment')
    plt.savefig(IMG_OUT_DIR / "graph2_sentiment_vs_return.png")
    plt.close()

    # --- 그래프 3: 상관관계 히트맵 ---
    plt.figure(figsize=(8, 6))
    corr_cols = ['Close', 'Volume', 'news_count', 'news_sentiment', 'volatility']
    sns.heatmap(df[corr_cols].corr(), annot=True, cmap='coolwarm', fmt=".2f")
    plt.title('Correlation Matrix')
    plt.savefig(IMG_OUT_DIR / "graph3_correlation.png")
    plt.close()
    
    print("   -> 그래프 3장 저장 완료 (src/out/graphs 폴더 확인)")

    # -----------------------------------------------------------
    # [과제 필수 3] 머신러닝 모델 학습
    # -----------------------------------------------------------
    print("\n✅ [3/3] 머신러닝 (Machine Learning) 수행 중...")
    
    # 1. 데이터 준비 (뉴스가 없었던 2006~초반 데이터가 너무 많으면 노이즈가 될 수 있음)
    # 여기서는 전체를 다 쓰되, 결측치만 제거
    ml_df = df.dropna().copy()
    
    # 2. Feature(X)와 Target(y)
    # 뉴스 정보와 전날의 거래 데이터를 보고 -> 내일 오를지(1) 내릴지(0) 예측
    features = ['news_count', 'news_sentiment', 'volatility', 'daily_return', 'Volume']
    X = ml_df[features]
    y = ml_df['target_up_down']
    
    # 3. 데이터 분리 (과거 데이터로 학습, 미래 데이터로 평가)
    # shuffle=False로 해야 시계열 순서가 유지됨
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False, random_state=42)
    
    # 4. 모델 학습 (Random Forest)
    model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    model.fit(X_train, y_train)
    
    # 5. 예측 및 평가
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"\n--- [Random Forest] 모델 평가 결과 ---")
    print(f"학습 기간: {X_train.index.min().date()} ~ {X_train.index.max().date()}")
    print(f"평가 기간: {X_test.index.min().date()} ~ {X_test.index.max().date()}")
    print(f"정확도 (Accuracy): {accuracy:.4f}")
    print("\n상세 보고서:")
    print(classification_report(y_test, y_pred))
    
    # 6. 변수 중요도 (어떤 게 예측에 중요했나?)
    importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("\n변수 중요도 (Top Features):")
    print(importances)
    
    # 중요도 그래프
    plt.figure(figsize=(8, 5))
    sns.barplot(x=importances.values, y=importances.index)
    plt.title('Feature Importance')
    plt.savefig(IMG_OUT_DIR / "graph4_feature_importance.png")
    plt.close()

    print("\n🎉 모든 과제 수행 완료!")

if __name__ == "__main__":
    main()
