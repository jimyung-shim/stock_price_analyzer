# **📈 NewsTrend Stock Insight: 뉴스 감성 기반 AWS 주가 예측 프로젝트**

## **1\. 프로젝트 개요 (Overview)**

### **📌 프로젝트 소개**

본 프로젝트는 글로벌 빅테크 기업인 \*\*Amazon(AWS)\*\*을 대상으로, **지난 15년(2006\~2021)간의 주가 데이터**와 **경제 뉴스 데이터**를 결합하여 분석한 연구 프로젝트이다.

뉴스 기사의 \*\*양(Volume)\*\*과 \*\*감성(Sentiment)\*\*이 실제 주가 변동성에 미치는 영향을 정량적으로 분석하고, 이를 머신러닝 모델에 적용하여 **최적의 매매 타이밍(상승/하락)을 예측**하는 것을 목표로 한다.

### **🎯 핵심 목표**

1. **데이터 융합:** 비정형 텍스트(뉴스)와 정형 시계열(주가) 데이터의 결합 및 전처리.  
2. **상관관계 규명:** 경제 뉴스의 감성이 주가 수익률과 변동성에 미치는 영향 분석.  
3. **예측 모델링:** Random Forest 알고리즘을 활용한 주가 등락 예측 (보조 지표로서의 뉴스 가치 검증).  
4. **서비스 설계:** 분석 결과를 활용한 개인 투자자용 매매 조언 서비스.

## **2\. 사용 데이터셋 (Data Source)**

본 프로젝트는 **Kaggle**의 검증된 데이터셋을 활용하여 데이터의 신뢰성을 확보하였다.

| 데이터셋 | 출처 | 기간 | 상세 내용 |
| :---- | :---- | :---- | :---- |
| **AWS 주가 데이터** | Kaggle | 2006.12 \~ 2021.10 | Open, High, Low, Close, Volume (일별 시계열) |
| **경제 뉴스 데이터** | Kaggle (CNBC) | 2006.12 \~ 2021.10 | 약 45만 건 중 Amazon/Economy 관련 기사 필터링 |

* **데이터 병합:** date를 기준으로 **Left Join**하여 final\_dataset\_2006\_2021.csv 생성.

## **3\. 프로젝트 수행 과정 (Process)**

### **🛠 Tech Stack**

* **Language:** Python 3.9+  
* **Data Processing:** Pandas, NumPy  
* **Visualization:** Matplotlib, Seaborn  
* **NLP (Sentiment):** TextBlob  
* **Machine Learning:** Scikit-learn (Random Forest Classifier)

### **📊 분석 단계**

1. **데이터 전처리 (Preprocessing)**  
   * 뉴스 텍스트 내 키워드('AWS', 'Fed', 'Inflation' 등) 필터링.  
   * TextBlob을 이용한 감성 점수 산출 (-1.0 \~ \+1.0).  
   * groupby를 통한 일별 기사 수 및 평균 감성 집계.  
   * 주가 변동성(High \- Low) 및 수익률(Close 등락률) 파생 변수 생성.  
2. **탐색적 데이터 분석 (EDA)**  
   * 연도별 뉴스 기사량 추이 시각화.  
   * 뉴스 감성과 주가 등락률 간의 산점도(Scatter Plot) 분석.  
   * 주요 변수 간 피어슨 상관계수(Heatmap) 분석.  
3. **머신러닝 모델링 (Modeling)**  
   * **모델:** Random Forest Classifier.  
   * **Target:** 익일 주가 상승 여부 (1: 상승, 0: 하락).  
   * **Features:** 거래량, 변동성, 전일 수익률, **뉴스 감성, 뉴스 기사 수**.

## **4\. 분석 결과 (Key Findings)**

### **📈 1\. 뉴스 기사량의 폭발적 증가**

2015년 이후 AWS 및 거시 경제 관련 뉴스 기사량이 급증하였다. 이는 시장 정보의 밀도가 높아졌으며, 주가 분석 시 뉴스 데이터의 중요성이 커졌음을 시사한다.  
(참조: src/out/graphs/graph1\_news\_volume\_trend.png)

### **📉 2\. 뉴스와 주가의 비선형적 관계**

뉴스 감성 점수와 주가 수익률의 상관계수는 약 0.03으로 나타났다. 이는 뉴스가 주가를 1:1로 설명하는 선형적 관계는 아니지만, 극단적인 감성(호재/악재)이 발생할 때 변동성이 커지는 경향을 확인했다.  
(참조: src/out/graphs/graph3\_correlation.png)

### **🤖 3\. 머신러닝 변수 중요도 (7%의 발견)**

Random Forest 모델 분석 결과, **뉴스 감성(Sentiment)은 약 7%의 중요도**를 기록했다.

* **1\~3위:** 거래량, 수익률, 변동성 (기술적 지표)  
* **4위:** **뉴스 감성 (심리적 지표)**  
* **결론:** 뉴스는 단독 지표로는 부족하지만, 기술적 분석과 결합할 때 예측 성능을 높이는 \*\*유의미한 보조 지표(Alpha)\*\*임이 입증되었다.

## **5\. 응용 서비스 설계: NewsTrend Stock Insight**

분석 결과를 바탕으로 실제 투자자에게 도움을 주는 앱 서비스를 설계했다.

### **📱 핵심 기능**

1. **AI 시장 심리 계기판 (Fear & Greed Index):**  
   * 매일 쏟아지는 뉴스의 감성을 분석하여 시장 분위기를 '공포\~과열' 5단계로 시각화.  
2. **변동성 주의보 (Volatility Alert):**  
   * 뉴스 기사량(news\_count)이 평소 대비 2배 이상 급증 시 푸시 알림 발송.  
3. **하이브리드 매매 신호 (The Hybrid Signal):**  
   * 기술적 상승 추세 \+ 뉴스 감성 긍정 조건이 동시에 만족될 때만 **"강력 매수"** 추천.

### **🏗 시스템 아키텍처**
```
graph LR  
    A\[뉴스/주가 데이터 수집\] \--\> B(Pandas 데이터 전처리)  
    B \--\> C{머신러닝 엔진}  
    C \--\>|감성 점수 추출| D\[시장 심리 분석\]  
    C \--\>|상승/하락 예측| E\[주가 예측 모델\]  
    D & E \--\> F\[사용자 모바일 앱\]
```
## **6\. 설치 및 실행 방법 (Usage)**

### **1\) 환경 설정**

\# 가상환경 생성 및 활성화  
python \-m venv .venv  
source .venv/bin/activate  \# Windows: .venv\\Scripts\\activate

\# 필수 라이브러리 설치  
pip install \-r requirements.txt

### **2\) 데이터 처리 및 분석 실행**

본 프로젝트는 모듈화된 파이썬 스크립트로 구성.

\# 1\. 뉴스 데이터 전처리 (정렬 및 감성 분석)  
python src/process\_news.py

\# 2\. 과거 데이터(2006-2021) 병합 및 데이터셋 생성  
python src/merge\_historical.py

\# 3\. 최종 분석 (통계, 시각화, 머신러닝 수행)  
python src/project\_analysis\_final.py

* 실행 후 src/out/graphs/ 폴더에서 분석 그래프를 확인 가능.

## **7\. 프로젝트 파일 구조**
```
stock\_price\_analyzer/  
├── README.md                  \# 프로젝트 설명서  
├── requirements.txt           \# 의존성 패키지 목록  
├── dataset/  
│   └── final\_dataset\_2006\_2021.csv  
├── raw/                       \# 원본 데이터 폴더 (Kaggle)  
│   ├── cnbc.txt  
│   └── stock\_data/  
│       └── Amazon stock data 2006.12-2021.10.csv  
└── src/                       \# 소스 코드  
    ├── process\_news.py        \# 뉴스 데이터 가공  
    ├── merge\_historical.py    \# 데이터 병합  
    ├── project\_analysis\_final.py \# 통계/시각화/ML 분석 메인  
    └── out/                   \# 결과물 저장소    
        └── graphs/            \# 분석 이미지 저장 경로
```
## **8\. 결론 (Conclusion)**

뉴스 텍스트 데이터는 주가 변동성을 설명하는 '제4의 변수'로서 충분한 잠재력을 가지고 있다. 본 프로젝트는 딥러닝(LSTM)이나 뉴스 출처별 가중치 적용 등으로 확장될 경우, 더욱 정교한 금융 예측 모델로 발전할 수 있다.
