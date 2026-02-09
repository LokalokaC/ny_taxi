from pathlib import Path
import pandas as pd
import re
import logging
import os

def delete_file(path: str) -> bool:
    p = Path(path)
    try:
        if p.exists():
            p.unlink()

        parent = p.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        return True
    except Exception:
        return False
    
def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', col).lower().strip() for col in df.columns]
    return df

def normalize_bq_name(name: str) -> str:

    s = re.sub(r'\W+', '_', name.strip())
    if not s:
        s = "field"
    if s[0].isdigit():
        s = f"f_{s}"
    return s[:300]

def configure_logging() -> None:
    if logging.getLogger().handlers:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
