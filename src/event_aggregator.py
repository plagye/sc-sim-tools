import json
import logging
import os
from pathlib import Path
from typing import Iterator
from dotenv import load_dotenv
import pandas as pd
import psycopg
from psycopg.rows import dict_row
from datetime import date
import re

log = logging.getLogger(__name__)
DATA_DIR = Path(__file__).parent.parent / "data"
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

def connect():
    load_dotenv()
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"],
        sslmode=os.environ["DB_SSL"],
        row_factory=dict_row,
    )

def discover_json_files(data_dir: Path) -> Iterator[Path]:
    for source in ["adm", "erp", "forecast", "tms"]:
        source_dir = data_dir / source
        if not source_dir.exists():
            log.warning("Source directory %s does not exist, skipping.", source_dir)
            continue
        for subdir in source_dir.iterdir():
            if not subdir.is_dir():
                continue
            if not DATE_PATTERN.match(subdir.name):
                log.warning("Skipping directory with unexpected name: %s", subdir)
                continue
            yield from subdir.glob("*.json")

def parse_json_file(file_path: Path) -> pd.DataFrame:
    with open(file_path) as f:
        data = json.load(f)
        
    if isinstance(data, dict):
        data = [data]

    records = []
    for event in data:
        records.append({
            "event_type": file_path.stem,
            "source_system": file_path.parts[-3],
            "event_date": date.fromisoformat(file_path.parent.name),
            "payload": json.dumps(event)
        })
    df = pd.DataFrame(records)
    return df

all_dfs = []

for file_path in discover_json_files(DATA_DIR):
    log.info("Processing file: %s", file_path)
    df = parse_json_file(file_path)
    all_dfs.append(df)

df = pd.concat(all_dfs, ignore_index=True)

INSERT_SQL = """
    INSERT INTO raw.events (event_type, source_system, event_date, payload)
    VALUES (%s, %s, %s, %s)
"""

def load_events_to_postgres(df):
    with connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                records = [
                    (row.event_type, row.source_system, row.event_date, row.payload)
                    for row in df.itertuples()
                ]
                cur.executemany(INSERT_SQL, records)
                log.info("Inserted %d events into raw.events", len(records))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_events_to_postgres(df)