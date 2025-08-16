
import os
import re
from datetime import datetime, timezone
from typing import List

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def to_snake_case(name: str) -> str:
    # Normalize column names to snake_case
    name = name.strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s\-\/]+", "_", name)
    return name.lower()

def list_csv_files(folder: str) -> List[str]:
    return [os.path.join(folder, f) for f in os.listdir(folder)
            if f.lower().endswith(".csv") and os.path.isfile(os.path.join(folder, f))]

def atomic_write(df, path: str, **to_csv_kwargs):
    # Write safely then rename
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, **to_csv_kwargs)
    os.replace(tmp, path)
