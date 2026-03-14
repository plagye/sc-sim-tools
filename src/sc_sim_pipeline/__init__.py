from dagster import Definitions

from sc_sim_pipeline.assets.bronze import raw_events, dim_tables
from sc_sim_pipeline.assets.silver import (
    stg_orders_asset, stg_erp_batch1_asset, stg_erp_batch2_asset,
    stg_erp_batch3_asset, stg_tms_asset, stg_adm_forecast_asset,
)
from sc_sim_pipeline.assets.gold import (
    gold_dimensions_asset, gold_fact_orders_asset, gold_fact_financial_tms_asset,
    gold_fact_inventory_asset, gold_aggregates_asset, gold_exports_asset,
)
from sc_sim_pipeline.resources.postgres import PostgresResource
from sc_sim_pipeline.sensors.sim_sensor import sim_data_sensor
from sc_sim_pipeline.jobs.pipeline_job import pipeline_job, agg_refresh_job
from sc_sim_pipeline.schedules.schedules import daily_full_run

defs = Definitions(
    assets=[
        raw_events, dim_tables,
        stg_orders_asset, stg_erp_batch1_asset, stg_erp_batch2_asset,
        stg_erp_batch3_asset, stg_tms_asset, stg_adm_forecast_asset,
        gold_dimensions_asset, gold_fact_orders_asset, gold_fact_financial_tms_asset,
        gold_fact_inventory_asset, gold_aggregates_asset, gold_exports_asset,
    ],
    resources={"postgres": PostgresResource()},
    sensors=[sim_data_sensor],
    jobs=[pipeline_job, agg_refresh_job],
    schedules=[daily_full_run],
)
