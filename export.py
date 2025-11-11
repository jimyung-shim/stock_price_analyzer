from __future__ import annotations
from pathlib import Path
from typing import Literal, Dict, Any, List
import json
import pandas as pd

def _save_table(df: pd.DataFrame, path: Path, fmt: Literal["parquet","csv"]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=True)
    else:
        df.to_csv(path, index=True)

def save_artifacts(
    *,
    prices: pd.DataFrame,
    indicators: pd.DataFrame,
    returns: pd.DataFrame,
    perf_rows: List[Dict[str, Any]],
    out_dir: str | Path,
    fmt: Literal["parquet","csv"] = "parquet",
) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    _save_table(prices, out / f"prices.{fmt}", fmt)
    _save_table(indicators, out / f"indicators.{fmt}", fmt)
    _save_table(returns, out / f"returns.{fmt}", fmt)
    with open(out / "performance.json", "w", encoding="utf-8") as f:
        json.dump(perf_rows, f, ensure_ascii=False, indent=2)
