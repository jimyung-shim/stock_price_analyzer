from __future__ import annotations
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from .analysis import compute_indicators, performance_summary

def _save_price_chart(df: pd.DataFrame, out: Path):
    fig, ax = plt.subplots()
    df["Adj Close"].plot(ax=ax, label="Adj Close")
    if "SMA20" in df: df["SMA20"].plot(ax=ax, label="SMA20")
    if "SMA50" in df: df["SMA50"].plot(ax=ax, label="SMA50")
    ax.set_title("Price (Adj Close) with SMAs")
    ax.set_xlabel("Date"); ax.set_ylabel("Price")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "price.png", dpi=160)
    plt.close(fig)

def _save_returns_hist(ret: pd.Series, out: Path):
    fig, ax = plt.subplots()
    ret.hist(ax=ax, bins=50)
    ax.set_title("Daily Returns Histogram")
    ax.set_xlabel("Return"); ax.set_ylabel("Frequency")
    fig.tight_layout()
    fig.savefig(out / "returns_hist.png", dpi=160)
    plt.close(fig)

def _save_drawdown_curve(df: pd.DataFrame, out: Path):
    if "DRAWDOWN" not in df: return
    fig, ax = plt.subplots()
    df["DRAWDOWN"].plot(ax=ax)
    ax.set_title("Drawdown")
    ax.set_xlabel("Date"); ax.set_ylabel("Drawdown")
    fig.tight_layout()
    fig.savefig(out / "drawdown.png", dpi=160)
    plt.close(fig)

def write_report(
    raw_df: pd.DataFrame,
    out_dir: str | Path,
    dataset_name: str,
    risk_free_rate_annual: float = 0.0,
) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # compute indicators & perf
    df = compute_indicators(raw_df)
    perf = performance_summary(raw_df, risk_free_rate_annual=risk_free_rate_annual)

    # save artifacts
    df.to_csv(out / "timeseries_with_indicators.csv")
    raw_df.to_csv(out / "raw_prices.csv")
    pd.DataFrame([perf.to_dict()]).to_csv(out / "performance_summary.csv", index=False)

    # charts
    _save_price_chart(df, out)
    if "RET_DAILY" in df:
        _save_returns_hist(df["RET_DAILY"].dropna(), out)
    _save_drawdown_curve(df, out)

    # markdown report
    md = []
    md.append(f"# Stock Price Analyzer — {dataset_name}\n")
    md.append("## Performance Summary")
    for k, v in perf.to_dict().items():
        if isinstance(v, float) and k in {"cagr", "total_return", "max_drawdown", "avg_daily_return", "std_daily_return"}:
            md.append(f"- **{k}**: {v:.4%}")
        else:
            md.append(f"- **{k}**: {v}")
    md.append("\n## Files")
    md.append("- `raw_prices.csv` — original OHLCV")
    md.append("- `timeseries_with_indicators.csv` — price + indicators")
    md.append("- `performance_summary.csv` — one-row metrics")
    md.append("- `price.png`, `returns_hist.png`, `drawdown.png` — charts\n")
    md.append("## Quick Previews")
    md.append("![Price](price.png)")
    md.append("![Returns Histogram](returns_hist.png)")
    md.append("![Drawdown](drawdown.png)")

    report_path = out / "report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")
    return report_path
