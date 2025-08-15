
from __future__ import annotations

import os
from include.utils import list_csv_files
from include.transform import transform_file
from include.scd_type1 import scd_type1_merge

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INBOX = os.path.join(BASE_DIR, "data", "inbox")
STAGING = os.path.join(BASE_DIR, "data", "staging")
TARGET = os.path.join(BASE_DIR, "data", "target")
ARCHIVE = os.path.join(BASE_DIR, "data", "archive")
SNAPSHOTS = os.path.join(TARGET, "snapshots")
MASTER = os.path.join(TARGET, "master.csv")

def main():
    files = list_csv_files(INBOX)
    if not files:
        print("No files in inbox. Put a CSV into data/inbox/ and rerun.")
        return
    files.sort(key=lambda p: os.path.getmtime(p))

    src = files[0]
    print("Processing:", src)
    fname = os.path.basename(src)
    cleaned_path = os.path.join(STAGING, f"cleaned_{fname}")
    meta = transform_file(src, cleaned_path, preferred_pk=None)
    print("Transform meta:", meta)

    result = scd_type1_merge(
        master_path=MASTER,
        incremental_path=cleaned_path,
        primary_key=meta["primary_key"],
        out_master_path=MASTER,
        snapshot_dir=SNAPSHOTS
    )
    print("Merge result:", result)

    # archive source
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    archived = os.path.join(ARCHIVE, f"{ts}__{fname}")
    os.replace(src, archived)
    print("Archived source to:", archived)

if __name__ == "__main__":
    main()
