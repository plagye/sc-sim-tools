"""Creates or replaces all export.* flat views for Power BI / Excel consumption."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db import connect

VIEWS = {
    "export.orders_with_customer_and_sku": """
        SELECT f.*, dc.company_name, dc.segment, dc.country_code, dc.region,
               ds.valve_type, ds.dn, ds.material, ds.pressure_class
        FROM mart.fact_order_lines f
        JOIN mart.dim_customer dc ON dc.customer_id = f.customer_id AND dc.is_current
        JOIN mart.dim_sku ds      ON ds.sku = f.sku AND ds.is_current
    """,
    "export.inventory_daily_flat": """
        SELECT i.*, ds.valve_type, ds.dn, ds.material, dw.name AS warehouse_name
        FROM mart.fact_inventory_daily i
        JOIN mart.dim_sku ds       ON ds.sku = i.sku AND ds.is_current
        JOIN mart.dim_warehouse dw ON dw.warehouse_code = i.warehouse_id
    """,
    "export.carrier_performance_flat": """
        SELECT s.*, dc.name AS carrier_name, dc.base_reliability,
               dc.transit_days_min, dc.transit_days_max, dc.cost_tier
        FROM mart.fact_shipments s
        JOIN mart.dim_carrier dc ON dc.carrier_code = s.carrier_code
    """,
    "export.demand_accuracy_flat": """
        SELECT * FROM mart.agg_demand_accuracy
    """,
    "export.customer_ar_flat": """
        SELECT p.*, dc.company_name, dc.segment, dc.region,
               dc.credit_limit, dc.payment_terms_days
        FROM mart.fact_payments p
        JOIN mart.dim_customer dc ON dc.customer_id = p.customer_id AND dc.is_current
    """,
}


def run():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS export")
            for view_name, query in VIEWS.items():
                cur.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
                print(f"Created view: {view_name}")
        conn.commit()


if __name__ == "__main__":
    run()
