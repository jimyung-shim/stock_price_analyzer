# Stock Price Analyzer (Pandas + yfinance)

Downloads OHLCV from Yahoo Finance and produces:
- `report.md` (performance summary with charts)
- `raw_prices.csv`
- `timeseries_with_indicators.csv` (SMA/EMA/RSI/MACD/Bollinger, returns, volatility, drawdown)
- `performance_summary.csv`
- charts: `price.png`, `returns_hist.png`, `drawdown.png`

## Install

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# or: pip install -e .
