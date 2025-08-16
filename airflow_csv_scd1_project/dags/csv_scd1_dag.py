from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd

# ---------- CONFIG ----------
BASE_DIR = "/home/vboxuser/Downloads/airflow_csv_scd1_project/data"
INBOX_DIR = os.path.join(BASE_DIR, "inbox")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
TARGET_DIR = os.path.join(BASE_DIR, "target")
PRIMARY_KEY = "CustomerID"   # <-- update this if your file has different PK
# ----------------------------

def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleanup + SCD Type 1 prep."""
    # Drop duplicates by primary key
    df = df.drop_duplicates(subset=[PRIMARY_KEY], keep="last")

    # Fill missing values
    df = df.fillna("UNKNOWN")

    # Add timestamps
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "create_datetime" not in df.columns:
        df["create_datetime"] = now
    df["update_datetime"] = now

    return df

def process_files():
    """Main function to handle new CSVs in INBOX."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(TARGET_DIR, exist_ok=True)

    files = [f for f in os.listdir(INBOX_DIR) if f.endswith(".csv")]
    if not files:
        print("📭 No new files found.")
        return

    for filename in files:
        src = os.path.join(INBOX_DIR, filename)
        print(f"➡️ Processing: {src}")

        df = pd.read_csv(src)
        print(f"✅ Read {len(df)} rows from {filename}")

        cleaned_df = clean_and_transform(df)

        # ---- SCD Type 1 logic ----
        target_file = os.path.join(TARGET_DIR, "target.csv")
        if os.path.exists(target_file):
            target_df = pd.read_csv(target_file)
            target_df = target_df.set_index(PRIMARY_KEY)
            cleaned_df = cleaned_df.set_index(PRIMARY_KEY)

            # Update existing rows
            target_df.update(cleaned_df)

            # Insert new rows
            new_rows = cleaned_df[~cleaned_df.index.isin(target_df.index)]
            final_df = pd.concat([target_df, new_rows], axis=0)
        else:
            final_df = cleaned_df.set_index(PRIMARY_KEY)

        # Save updated target
        final_df.reset_index().to_csv(target_file, index=False)
        print(f"💾 Target updated at {target_file}")

        # Archive original file
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        archived = os.path.join(ARCHIVE_DIR, f"{timestamp}__{filename}")
        shutil.move(src, archived)
        print(f"📦 Archived to {archived}")


# ---------------- DAG DEFINITION ----------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="csv_scd1_pipeline",
    default_args=default_args,
    description="CSV pipeline with SCD Type 1 using Airflow",
    schedule="*/5 * * * *",   # every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:

    task_process_files = PythonOperator(
        task_id="process_csv_files",
        python_callable=process_files,
    )
