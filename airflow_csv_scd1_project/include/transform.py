
from __future__ import annotations

import os
import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict
from datetime import datetime, timezone
from .utils import to_snake_case, utc_now_iso

PRIMARY_KEY_CANDIDATES = [
    "id", "subscription_id", "customer_id", "user_id", "policy_id", "account_id"
]

def read_csv_any(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {c: to_snake_case(str(c)) for c in df.columns}
    return df.rename(columns=rename_map)

def parse_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Try parsing columns that look like dates
    for col in df.columns:
        lc = str(col).lower()
        if any(tag in lc for tag in ["date", "datetime", "_at"]):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
            except Exception:
                pass
    return df

def detect_primary_key(df: pd.DataFrame, preferred: Optional[str] = None) -> Optional[str]:
    if preferred and preferred in df.columns and not df[preferred].isnull().any() and df[preferred].nunique() == len(df):
        return preferred
    # Try known candidates
    for c in PRIMARY_KEY_CANDIDATES:
        if c in df.columns and not df[c].isnull().any() and df[c].nunique() == len(df):
            return c
    # Fallback: find any column that is all-unique and non-null
    for c in df.columns:
        if not df[c].isnull().any() and df[c].nunique() == len(df):
            return c
    return None

def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    # Numeric -> median
    for col in df.select_dtypes(include=[np.number]).columns:
        med = df[col].median()
        df[col] = df[col].fillna(med)
    # Datetime -> forward-fill then back-fill (keeps chronology) if column exists
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        if df[col].isnull().any():
            df[col] = df[col].ffill().bfill()
    # Strings -> 'UNKNOWN' after trimming
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": np.nan})
        mode = df[col].mode(dropna=True)
        fill_value = str(mode.iloc[0]) if not mode.empty else "UNKNOWN"
        df[col] = df[col].fillna(fill_value)
    return df

def dedupe(df: pd.DataFrame, primary_key: Optional[str]) -> pd.DataFrame:
    # Remove exact duplicates first
    df = df.drop_duplicates()
    # If primary key known, keep the last occurrence (assuming later rows are newer)
    if primary_key and primary_key in df.columns:
        df = df.drop_duplicates(subset=[primary_key], keep="last")
    return df

def transform_file(input_path: str, output_path: str, preferred_pk: Optional[str] = None) -> Dict:
    """
    Cleans the CSV:
      - standardize column names
      - parse datetime-ish columns
      - detect primary key (or use preferred)
      - remove duplicates (favor later rows for the same PK)
      - fill missing values (numeric median, datetime ffill/bfill, strings mode/UNKNOWN)
      - add 'ingest_datetime_utc' to track this run
    """
    df = read_csv_any(input_path)
    df = standardize_columns(df)
    df = parse_datetime_columns(df)
    pk = detect_primary_key(df, preferred=preferred_pk)
    df = dedupe(df, pk)
    df = fill_missing_values(df)

    # add ingest time
    now_iso = utc_now_iso()
    df["ingest_datetime_utc"] = now_iso

    # Ensure primary key is present for downstream merge
    if pk is None:
        raise ValueError("Could not detect a primary key. Please specify one via preferred_pk.")

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    # Return metadata
    return {
        "output_path": output_path,
        "primary_key": pk,
        "row_count": len(df),
        "ingest_datetime_utc": now_iso,
        "columns": list(df.columns)
    }
