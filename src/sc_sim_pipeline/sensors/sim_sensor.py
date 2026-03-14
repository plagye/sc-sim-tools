import os
from pathlib import Path

from dotenv import load_dotenv
from dagster import RunRequest, SkipReason, SensorEvaluationContext, sensor

from sc_sim_pipeline.jobs.pipeline_job import pipeline_job

load_dotenv(Path(__file__).parents[4] / ".env")


@sensor(job=pipeline_job, minimum_interval_seconds=30)
def sim_data_sensor(context: SensorEvaluationContext):
    """Watches DATA_DIR for new simulation files. Triggers the full pipeline
    (raw_events → silver → gold) whenever the newest file mtime advances."""
    data_dir = Path(os.environ["DATA_DIR"])

    if not data_dir.exists():
        yield SkipReason(f"DATA_DIR does not exist: {data_dir}")
        return

    mtimes = [f.stat().st_mtime for f in data_dir.rglob("*.json")]
    if not mtimes:
        yield SkipReason("No JSON files found in DATA_DIR")
        return

    newest_mtime = max(mtimes)
    last_mtime = float(context.cursor) if context.cursor else 0.0

    if newest_mtime > last_mtime:
        context.update_cursor(str(newest_mtime))
        yield RunRequest(
            run_key=str(newest_mtime),
            tags={"trigger": "sim_sensor", "newest_mtime": str(int(newest_mtime))},
        )
    else:
        yield SkipReason(f"No new files since last check")
