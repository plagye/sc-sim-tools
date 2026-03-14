from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext
from sc_sim_pipeline.resources.postgres import PostgresResource


@sensor(minimum_interval_seconds=30)
def sim_data_sensor(context: SensorEvaluationContext, postgres: PostgresResource):
    try:
        with postgres.get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(ingested_at)::text AS latest, COUNT(*) AS n FROM raw.ingested_files"
            ).fetchone()
    except Exception as e:
        yield SkipReason(f"raw.ingested_files not accessible: {e}")
        return

    latest = row["latest"] if row and row["n"] > 0 else None
    if latest is None:
        yield SkipReason("No files ingested yet")
        return

    last_ts = context.cursor or ""
    if latest > last_ts:
        context.update_cursor(latest)
        yield RunRequest(
            run_key=latest,
            tags={"trigger": "sim_sensor", "ingested_at": latest},
        )
    else:
        yield SkipReason(f"No new files since {last_ts}")
