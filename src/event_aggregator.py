import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Iterator
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from datetime import date
import re

load_dotenv()
log = logging.getLogger(__name__)
DATA_DIR = Path(os.environ["DATA_DIR"])

# Matches YYYY-MM-DD and YYYY-MM date folder names
DATE_PATTERN = re.compile(r"\d{4}-\d{2}(-\d{2})?$")
# Extracts date from demand_signal filenames: SIG-YYYYMMDD-NNNN.json
SIG_DATE_RE = re.compile(r"SIG-(\d{4})(\d{2})(\d{2})-")

ENSURE_INGESTED_FILES_TABLE = """
    CREATE TABLE IF NOT EXISTS raw.ingested_files (
        file_path   TEXT PRIMARY KEY,
        file_hash   CHAR(64) NOT NULL,
        ingested_at TIMESTAMPTZ DEFAULT now(),
        row_count   INTEGER NOT NULL
    )
"""

INSERT_EVENT_SQL = """
    INSERT INTO raw.events (event_type, source_system, event_date, payload)
    VALUES (%s, %s, %s, %s)
"""

UPSERT_FILE_SQL = """
    INSERT INTO raw.ingested_files (file_path, file_hash, row_count)
    VALUES (%s, %s, %s)
    ON CONFLICT (file_path) DO UPDATE SET
        file_hash   = EXCLUDED.file_hash,
        row_count   = EXCLUDED.row_count,
        ingested_at = now()
"""


def connect():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"],
        sslmode=os.environ["DB_SSL"],
        row_factory=dict_row,
    )


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def discover_json_files(data_dir: Path) -> Iterator[Path]:
    for source in ["adm", "erp", "forecast", "tms"]:
        source_dir = data_dir / source
        if not source_dir.exists():
            log.warning("Source directory %s does not exist, skipping.", source_dir)
            continue
        for subdir in source_dir.iterdir():
            if not subdir.is_dir():
                continue
            if DATE_PATTERN.match(subdir.name):
                yield from subdir.glob("*.json")
            elif subdir.name == "demand_signals":
                # Individual signal files not in date subdirs; date is in filename
                yield from subdir.glob("*.json")
            else:
                log.warning("Skipping directory with unexpected name: %s", subdir)


def _event_date_from_path(file_path: Path) -> date:
    folder = file_path.parent.name
    if folder == "demand_signals":
        m = SIG_DATE_RE.match(file_path.name)
        if not m:
            raise ValueError(f"Cannot parse date from demand_signal filename: {file_path.name}")
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    try:
        return date.fromisoformat(folder)
    except ValueError:
        # monthly folder YYYY-MM — use first day of the month
        return date.fromisoformat(folder + "-01")


def parse_records(file_path: Path) -> list[tuple]:
    with open(file_path) as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = [data]

    event_type = "demand_signals" if file_path.parent.name == "demand_signals" else file_path.stem
    # parts[-3] resolves to the domain (erp/tms/adm/forecast) for both
    # date-subfolder files (.../erp/2026-01-06/file.json)
    # and demand_signals (.../adm/demand_signals/SIG-....json)
    source_system = file_path.parts[-3]
    event_date = _event_date_from_path(file_path)

    seen = set()
    records = []
    for event in data:
        payload = json.dumps(event, sort_keys=True)
        h = hashlib.sha256(payload.encode()).hexdigest()
        if h in seen:
            log.debug("Duplicate payload skipped in %s", file_path.name)
            continue
        seen.add(h)
        records.append((event_type, source_system, event_date, payload))
    return records


def load_files(data_dir: Path) -> None:
    with connect() as conn:
        conn.execute(ENSURE_INGESTED_FILES_TABLE)
        conn.commit()

        ingested = {
            row["file_path"]: row["file_hash"]
            for row in conn.execute(
                "SELECT file_path, file_hash FROM raw.ingested_files"
            ).fetchall()
        }

        new_count = 0
        skipped_count = 0

        for file_path in discover_json_files(data_dir):
            path_str = str(file_path)
            current_hash = file_sha256(file_path)

            if ingested.get(path_str) == current_hash:
                skipped_count += 1
                continue

            try:
                records = parse_records(file_path)
            except Exception:
                log.exception("Failed to parse %s — skipping", file_path)
                continue

            if not records:
                log.warning("No records parsed from %s", file_path)
                continue

            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.executemany(INSERT_EVENT_SQL, records)
                    conn.execute(UPSERT_FILE_SQL, (path_str, current_hash, len(records)))
                log.info("Ingested %s — %d events", file_path.name, len(records))
                new_count += 1
            except Exception:
                log.exception("Failed to ingest %s — skipping", file_path)

        log.info("Done — new: %d, skipped: %d", new_count, skipped_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_files(DATA_DIR)
