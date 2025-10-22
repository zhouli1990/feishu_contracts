from __future__ import annotations
from typing import Dict, List, Any
import pandas as pd


def validate_required(rows: List[Dict[str, Any]], required_columns: List[str]) -> List[Dict[str, Any]]:
    errors: List[Dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        for col in required_columns:
            val = row.get(col, None)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                errors.append({
                    "row": i,
                    "column": col,
                    "error": "required_missing",
                    "value": val,
                })
    return errors


def validate_unique(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    errors: List[Dict[str, Any]] = []
    seen = set()
    for i, row in enumerate(rows, start=1):
        k = row.get(key)
        if k in seen:
            errors.append({"row": i, "column": key, "error": "duplicate_key", "value": k})
        else:
            seen.add(k)
    return errors


def dataframe_required(df: pd.DataFrame, cols: List[str]) -> List[str]:
    missing = [c for c in cols if c not in df.columns]
    return missing
