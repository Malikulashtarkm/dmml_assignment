
from __future__ import annotations

import os
import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
from .utils import utc_now_iso, atomic_write

CREATE_COL = "create_datetime_utc"
UPDATE_COL = "update_datetime_utc"

def _ensure_datetime_columns(df: pd.DataFrame, default_now_iso: str) -> pd.DataFrame:
    if CREATE_COL not in df.columns:
        df[CREATE_COL] = default_now_iso
    if UPDATE_COL not in df.columns:
        df[UPDATE_COL] = default_now_iso
    return df

def _align_columns(master: pd.DataFrame, inc: pd.DataFrame, keep_cols=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Union of columns, but ensure SCD columns are present
    cols = sorted(set(master.columns).union(set(inc.columns)))
    if keep_cols:
        for c in keep_cols:
            if c not in cols:
                cols.append(c)
    master = master.reindex(columns=cols, fill_value=np.nan)
    inc = inc.reindex(columns=cols, fill_value=np.nan)
    return master, inc

def scd_type1_merge(master_path: str, incremental_path: str, primary_key: str,
                    out_master_path: Optional[str] = None, snapshot_dir: Optional[str] = None) -> Dict:
    """
    Implements SCD Type 1 on CSV files:
      - Overwrite existing rows by primary key with new values
      - Insert new rows not present in master
      - Preserve existing create_datetime_utc for updated rows
      - Set update_datetime_utc to current time for updated/inserted rows
      - Write a new versioned snapshot and update the master
    """
    now_iso = utc_now_iso()
    # Read incremental
    inc = pd.read_csv(incremental_path)
    if primary_key not in inc.columns:
        raise ValueError(f"Primary key '{primary_key}' not found in incremental file.")

    # Dedup inc by primary key (last wins)
    inc = inc.drop_duplicates(subset=[primary_key], keep="last")

    # Load or create master
    if os.path.exists(master_path):
        master = pd.read_csv(master_path)
    else:
        master = pd.DataFrame(columns=inc.columns)

    master = _ensure_datetime_columns(master, default_now_iso=now_iso)
    master, inc = _align_columns(master, inc, keep_cols=[CREATE_COL, UPDATE_COL])

    # Set index for efficient alignment
    master = master.set_index(primary_key, drop=False)
    inc = inc.set_index(primary_key, drop=False)

    updated_keys = master.index.intersection(inc.index)
    new_keys = inc.index.difference(master.index)

    # Overwrite (SCD1) for updated keys, preserving create time
    if len(updated_keys) > 0:
        # preserve create time
        master.loc[updated_keys, UPDATE_COL] = now_iso
        # For all non-SCD columns, copy values from inc
        for col in inc.columns:
            if col in (CREATE_COL, UPDATE_COL):
                continue
            master.loc[updated_keys, col] = inc.loc[updated_keys, col].values

    # Inserts for new keys
    if len(new_keys) > 0:
        new_rows = inc.loc[new_keys].copy()
        new_rows[CREATE_COL] = now_iso
        new_rows[UPDATE_COL] = now_iso
        master = pd.concat([master, new_rows], axis=0)

    # Sort by primary key for stability
    master = master.sort_index()

    # Prepare paths
    out_master_path = out_master_path or master_path
    os.makedirs(os.path.dirname(out_master_path), exist_ok=True)

    # Write master atomically
    atomic_write(master.reset_index(drop=True), out_master_path)

    # Write snapshot
    snapshot_path = None
    if snapshot_dir:
        os.makedirs(snapshot_dir, exist_ok=True)
        from datetime import datetime
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        base = os.path.splitext(os.path.basename(out_master_path))[0]
        snapshot_path = os.path.join(snapshot_dir, f"{base}_{ts}.csv")
        atomic_write(master.reset_index(drop=True), snapshot_path)

    return {
        "master_path": out_master_path,
        "snapshot_path": snapshot_path,
        "updated_count": int(len(updated_keys)),
        "inserted_count": int(len(new_keys)),
        "row_count_after": int(len(master)),
        "timestamp_utc": now_iso
    }
