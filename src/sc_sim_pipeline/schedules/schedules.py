from dagster import ScheduleDefinition
from sc_sim_pipeline.jobs.pipeline_job import pipeline_job

daily_full_run = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 0 * * *",
    execution_timezone="Europe/Warsaw",
)
