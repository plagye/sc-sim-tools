import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext, MetadataValue

load_dotenv(Path(__file__).parents[4] / ".env")
sys.path.insert(0, str(Path(__file__).parents[3]))

from event_aggregator import load_files
from load_master import load_dim
from sc_sim_pipeline.resources.postgres import PostgresResource


@asset(group_name="bronze")
def raw_events(context: AssetExecutionContext, postgres: PostgresResource):
    data_dir = Path(os.environ["DATA_DIR"])
    load_files(data_dir)
    with postgres.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM raw.events").fetchone()["n"]
        max_date = conn.execute("SELECT MAX(event_date) AS d FROM raw.events").fetchone()["d"]
        new_files = conn.execute("SELECT COUNT(*) AS n FROM raw.ingested_files").fetchone()["n"]
    context.add_output_metadata({
        "total_events": MetadataValue.int(total),
        "ingested_files": MetadataValue.int(new_files),
        "max_event_date": MetadataValue.text(str(max_date)),
    })


@asset(group_name="bronze", deps=[raw_events])
def dim_tables(context: AssetExecutionContext, postgres: PostgresResource):
    load_dim()
    with postgres.get_connection() as conn:
        counts = {}
        for tbl in ["dim.carriers", "dim.catalog", "dim.customers", "dim.warehouses"]:
            counts[tbl] = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})
