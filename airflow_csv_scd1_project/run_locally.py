import os
import shutil
from datetime import datetime
import pandas as pd

# Paths
BASE_DIR = "/home/vboxuser/Downloads/airflow_csv_scd1_project/data"
INBOX_DIR = os.path.join(BASE_DIR, "inbox")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
TARGET_DIR = os.path.join(BASE_DIR, "target")

PRIMARY_KEY = "CustomerID"  # change if needed

def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=[PRIMARY_KEY], keep="last")
    df = df.fillna("NA")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "create_datetime" not in df.columns:
        df["create_datetime"] = now_str
    df["update_datetime"] = now_str
    return df

def scd_type1_merge(new_df: pd.DataFrame, target_path: str) -> pd.DataFrame:
    if os.path.exists(target_path):
        old_df = pd.read_csv(target_path)
        merged_df = pd.concat([old_df, new_df]).drop_duplicates(subset=[PRIMARY_KEY], keep="last")
    else:
        merged_df = new_df
    return merged_df

def main():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(TARGET_DIR, exist_ok=True)

    files = os.listdir(INBOX_DIR)
    print(f"📂 Files found in inbox: {files}")

    for file_name in files:
        if not file_name.lower().endswith(".csv"):
            print(f"Skipping non-csv file: {file_name}")
            continue

        src = os.path.join(INBOX_DIR, file_name)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        archived = os.path.join(ARCHIVE_DIR, f"{timestamp}__{file_name}")
        target_path = os.path.join(TARGET_DIR, file_name)

        print(f"➡️ Processing file: {src}")

        df = pd.read_csv(src)
        print(f"✅ Read {len(df)} rows from {file_name}")

        cleaned_df = clean_and_transform(df)
        final_df = scd_type1_merge(cleaned_df, target_path)

        final_df.to_csv(target_path, index=False)
        shutil.copy2(src, archived)

        print(f"🎯 Target updated: {target_path}")
        print(f"📦 Archived copy saved: {archived}")

if __name__ == "__main__":
    main()
