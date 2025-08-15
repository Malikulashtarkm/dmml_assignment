
from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

# Update this base_dir to wherever you mount your project folders in Airflow
BASE_DIR = os.environ.get("CSV_SCD1_BASE", "/opt/airflow/airflow_csv_scd1_project")

INBOX_DIR = os.path.join(BASE_DIR, "data", "inbox")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
TARGET_DIR = os.path.join(BASE_DIR, "data", "target")
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archive")
SNAPSHOT_DIR = os.path.join(TARGET_DIR, "snapshots")

MASTER_FILE = os.path.join(TARGET_DIR, "master.csv")

FILE_PATTERN = ".csv"         # process any CSV file dropped in INBOX_DIR
PREFERRED_PK = os.environ.get("CSV_PRIMARY_KEY", None)  # e.g. "subscription_id"

# Import our helpers (ensure include/ is on PYTHONPATH via Airflow or set PYTHONPATH env)
from include.transform import transform_file
from include.scd_type1 import scd_type1_merge
from include.utils import list_csv_files

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["csv", "scd1", "files", "pandas"]
)
def csv_scd1_pipeline():
    @task
    def pick_new_file() -> str:
        files = list_csv_files(INBOX_DIR)
        if not files:
            raise AirflowSkipException("No files to process right now.")
        # pick the oldest to maintain order
        files.sort(key=lambda p: os.path.getmtime(p))
        return files[0]

    @task
    def transform_task(src_path: str) -> dict:
        fname = os.path.basename(src_path)
        out_path = os.path.join(STAGING_DIR, f"cleaned_{fname}")
        meta = transform_file(src_path, out_path, preferred_pk=PREFERRED_PK)
        return meta

    @task
    def scd_merge_task(meta: dict) -> dict:
        cleaned_path = meta["output_path"]
        primary_key = meta["primary_key"]
        result = scd_type1_merge(
            master_path=MASTER_FILE,
            incremental_path=cleaned_path,
            primary_key=primary_key,
            out_master_path=MASTER_FILE,
            snapshot_dir=SNAPSHOT_DIR
        )
        return result

    @task
    def archive_source(src_path: str) -> str:
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        base = os.path.basename(src_path)
        # timestamp the archived filename
        from datetime import datetime
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        dst = os.path.join(ARCHIVE_DIR, f"{ts}__{base}")
        os.replace(src_path, dst)  # move
        return dst

    src_path = pick_new_file()
    meta = transform_task(src_path)
    merge_info = scd_merge_task(meta)
    archived = archive_source(src_path)

csv_scd1_pipeline()
