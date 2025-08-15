
# CSV → Clean → SCD Type 1 → Versioned Master (Airflow)

This mini-project watches a folder for new CSVs every 5 minutes, cleans them with a Python transform, and merges into a **Type 1 SCD** master CSV. Each run also writes a **versioned snapshot** of the master for audit/version control.

## What it does

1. **Detects a new CSV** dropped in `data/inbox/`.
2. **Transforms** it with pandas:
   - Standardizes column names to `snake_case`.
   - Parses any column that looks like a date (e.g., contains `date`, `datetime`, `_at`).
   - Removes duplicate rows; for duplicate primary keys, **last row wins**.
   - Fills missing values:
     - Numeric → column median
     - Datetime → forward-fill then back-fill
     - Text → most frequent value (fallback `"UNKNOWN"`)
   - Adds `ingest_datetime_utc` (timestamp of this run).
3. **Merges** into a master CSV using **SCD Type 1**:
   - Overwrites existing rows by primary key with new values.
   - Inserts new keys.
   - Preserves `create_datetime_utc` for existing rows.
   - Sets `update_datetime_utc` for new/updated rows.
4. **Writes a fresh `master.csv`** and a timestamped **snapshot** under `data/target/snapshots/`.
5. **Archives the source file** under `data/archive/` with a timestamp prefix.

## Project layout

```
airflow_csv_scd1_project/
├─ dags/
│  └─ scd1_csv_pipeline.py
├─ include/
│  ├─ transform.py
│  ├─ scd_type1.py
│  └─ utils.py
└─ data/
   ├─ inbox/       # drop incoming CSVs here
   ├─ staging/     # cleaned CSVs land here
   ├─ target/      # master.csv lives here
   │  └─ snapshots/
   └─ archive/     # source files get moved here after processing
```

## Running locally (no Airflow)

1. Put your CSVs in `data/inbox/`. A copy of your uploaded sample has been placed there.
2. Run the local test script:

```bash
python run_locally.py
```

It will transform the first file in `inbox/`, merge into `target/master.csv`, write a snapshot, and archive the source file.

## Airflow setup

1. Copy the **entire** `airflow_csv_scd1_project/` directory into your Airflow environment (e.g., `/opt/airflow/airflow_csv_scd1_project/`).
2. Ensure Airflow's PYTHONPATH can import from `include/` (typical if project is mounted into `/opt/airflow`).
3. Set env vars (optional):
   - `CSV_SCD1_BASE` → base folder path (default `/opt/airflow/airflow_csv_scd1_project`)
   - `CSV_PRIMARY_KEY` → primary key column (snake_case). If not set, auto-detection is attempted.
4. Start the webserver & scheduler. The DAG `csv_scd1_pipeline` will poll every 5 minutes.
5. Drop files into `${CSV_SCD1_BASE}/data/inbox/`.

## Notes & tweaks

- **Primary key:** auto-detection tries common names like `subscription_id`, `customer_id`, etc., then any all-unique non-null column. For your sample, it's `Subscription_ID` ⟶ `subscription_id` after normalization.
- **Idempotency:** Source files are moved to `archive/` after successful processing to avoid reprocessing.
- **Versioning:** Each run writes a full snapshot of the master to `data/target/snapshots/master_YYYYMMDDTHHMMSS.csv`.
- **Atomic writes:** Master and snapshot are written atomically (temp file then rename).
- **Time zone:** All timestamps are in **UTC** to avoid DST/offset issues.

## Requirements

- Python 3.9+
- pandas
- Airflow 2.6+ (for DAG execution), though the pipeline code itself just uses pandas.

