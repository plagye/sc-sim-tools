import sys
from pathlib import Path
from dagster import asset, AssetExecutionContext, MetadataValue

sys.path.insert(0, str(Path(__file__).parents[3]))

import stg_orders
import stg_erp_batch1
import stg_erp_batch2
import stg_erp_batch3
import stg_tms
import stg_adm_forecast
from sc_sim_pipeline.resources.postgres import PostgresResource


@asset(group_name="silver", deps=["dim_tables"])
def stg_orders_asset(context: AssetExecutionContext, postgres: PostgresResource):
    stg_orders.run()
    with postgres.get_connection() as conn:
        orders = conn.execute("SELECT COUNT(*) AS n FROM staging.orders").fetchone()["n"]
        lines = conn.execute("SELECT COUNT(*) AS n FROM staging.order_lines").fetchone()["n"]
    context.add_output_metadata({
        "staging.orders": MetadataValue.int(orders),
        "staging.order_lines": MetadataValue.int(lines),
    })


@asset(group_name="silver", deps=["dim_tables"])
def stg_erp_batch1_asset(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        counts = {}
        counts["exchange_rates"] = stg_erp_batch1.process_exchange_rates(conn)
        counts["inventory_snapshots"] = stg_erp_batch1.process_inventory_snapshots(conn)
        counts["inventory_movements"] = stg_erp_batch1.process_inventory_movements(conn)
        counts["credit_events"] = stg_erp_batch1.process_credit_events(conn)
        counts["production_completions"] = stg_erp_batch1.process_production_completions(conn)
        counts["backorder_events"] = stg_erp_batch1.process_backorder_events(conn)
    context.add_output_metadata({k: MetadataValue.int(v or 0) for k, v in counts.items()})


@asset(group_name="silver", deps=["dim_tables"])
def stg_erp_batch2_asset(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        counts = {}
        counts["payments"] = stg_erp_batch2.load_payments(conn)
        counts["master_data_changes"] = stg_erp_batch2.load_master_data_changes(conn)
        counts["production_reclassifications"] = stg_erp_batch2.load_production_reclassifications(conn)
        counts["order_cancellations"] = stg_erp_batch2.load_order_cancellations(conn)
        counts["order_modifications"] = stg_erp_batch2.load_order_modifications(conn)
        counts["schema_evolution_events"] = stg_erp_batch2.load_schema_evolution_events(conn)
    context.add_output_metadata({k: MetadataValue.int(v or 0) for k, v in counts.items()})


@asset(group_name="silver", deps=["dim_tables"])
def stg_erp_batch3_asset(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        n = stg_erp_batch3.load_supply_disruptions(conn)
    context.add_output_metadata({"staging.supply_disruptions": MetadataValue.int(n or 0)})


@asset(group_name="silver", deps=["dim_tables"])
def stg_tms_asset(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        stg_tms.load_loads(conn)
        stg_tms.load_carrier_events(conn)
        stg_tms.load_returns(conn)
        counts = {
            "staging.loads": conn.execute("SELECT COUNT(*) AS n FROM staging.loads").fetchone()["n"],
            "staging.carrier_events": conn.execute("SELECT COUNT(*) AS n FROM staging.carrier_events").fetchone()["n"],
            "staging.returns": conn.execute("SELECT COUNT(*) AS n FROM staging.returns").fetchone()["n"],
            "staging.return_lines": conn.execute("SELECT COUNT(*) AS n FROM staging.return_lines").fetchone()["n"],
        }
    context.add_output_metadata({k: MetadataValue.int(v) for k, v in counts.items()})


@asset(group_name="silver", deps=["dim_tables"])
def stg_adm_forecast_asset(context: AssetExecutionContext, postgres: PostgresResource):
    # stg_adm_forecast uses positional row access (fetchone()[0]), needs tuple rows
    with postgres.get_tuple_connection() as conn:
        counts = {}
        counts["demand_plans"] = stg_adm_forecast.load_demand_plans(conn)
        counts["inventory_targets"] = stg_adm_forecast.load_inventory_targets(conn)
        counts["demand_signals"] = stg_adm_forecast.load_demand_signals(conn)
        counts["demand_forecasts"] = stg_adm_forecast.load_demand_forecasts(conn)
    context.add_output_metadata({k: MetadataValue.int(v or 0) for k, v in counts.items()})
