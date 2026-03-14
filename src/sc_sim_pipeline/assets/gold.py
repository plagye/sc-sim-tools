import sys
from pathlib import Path
from dagster import asset, AssetExecutionContext, MetadataValue

sys.path.insert(0, str(Path(__file__).parents[3]))

import gold_dimensions
import gold_fact_orders
import gold_fact_inventory_production
import gold_fact_financial_tms
import gold_aggregates
import gold_exports
from sc_sim_pipeline.resources.postgres import PostgresResource


@asset(group_name="gold", deps=[
    "stg_orders_asset", "stg_erp_batch1_asset", "stg_erp_batch2_asset",
    "stg_erp_batch3_asset", "stg_tms_asset", "stg_adm_forecast_asset",
])
def gold_dimensions_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_dimensions.run()
    with postgres.get_connection() as conn:
        counts = {t: conn.execute(f"SELECT COUNT(*) AS n FROM mart.{t}").fetchone()["n"]
                  for t in ["dim_date", "dim_customer", "dim_sku", "dim_carrier", "dim_warehouse"]}
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})


@asset(group_name="gold", deps=["gold_dimensions_asset"])
def gold_fact_orders_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_fact_orders.run()
    with postgres.get_connection() as conn:
        n = conn.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines").fetchone()["n"]
    context.add_output_metadata({"mart.fact_order_lines": MetadataValue.int(n)})


@asset(group_name="gold", deps=["gold_dimensions_asset"])
def gold_fact_financial_tms_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_fact_financial_tms.run()
    with postgres.get_connection() as conn:
        counts = {t: conn.execute(f"SELECT COUNT(*) AS n FROM mart.{t}").fetchone()["n"]
                  for t in ["fact_shipments", "fact_payments", "fact_credit_events", "fact_returns"]}
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})


@asset(group_name="gold", deps=["gold_dimensions_asset"])
def gold_fact_inventory_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_fact_inventory_production.run()
    with postgres.get_connection() as conn:
        counts = {t: conn.execute(f"SELECT COUNT(*) AS n FROM mart.{t}").fetchone()["n"]
                  for t in ["fact_inventory_daily", "fact_production"]}
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})


@asset(group_name="gold", deps=["gold_fact_orders_asset", "gold_fact_financial_tms_asset", "gold_fact_inventory_asset"])
def gold_aggregates_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_aggregates.main()
    with postgres.get_connection() as conn:
        counts = {t: conn.execute(f"SELECT COUNT(*) AS n FROM mart.{t}").fetchone()["n"]
                  for t in ["agg_order_metrics_daily", "agg_customer_scorecard", "agg_sku_performance",
                            "agg_carrier_scorecard", "agg_inventory_health", "agg_demand_accuracy",
                            "agg_cash_flow_monthly", "agg_production_quality"]}
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})


@asset(group_name="gold", deps=["gold_aggregates_asset"])
def gold_exports_asset(context: AssetExecutionContext, postgres: PostgresResource):
    gold_exports.run()
    context.add_output_metadata({"views_created": MetadataValue.int(5)})
