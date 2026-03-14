import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()


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


def agg_order_metrics_daily(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_order_metrics_daily (
            agg_date                   DATE PRIMARY KEY,
            orders_placed              INTEGER,
            lines_placed               INTEGER,
            revenue_pln                NUMERIC(15,4),
            qty_shipped_total          INTEGER,
            qty_ordered_total          INTEGER,
            qty_backordered_total      INTEGER,
            fill_rate                  NUMERIC(8,4),
            backorder_rate             NUMERIC(8,4),
            is_express_escalated_count INTEGER
        )
    """)
    cur.execute("TRUNCATE mart.agg_order_metrics_daily RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_order_metrics_daily (
            agg_date, orders_placed, lines_placed, revenue_pln,
            qty_shipped_total, qty_ordered_total, qty_backordered_total,
            fill_rate, backorder_rate, is_express_escalated_count
        )
        SELECT
            order_date                                                          AS agg_date,
            COUNT(DISTINCT order_id)                                            AS orders_placed,
            COUNT(*)                                                            AS lines_placed,
            SUM(line_revenue_pln)                                               AS revenue_pln,
            SUM(qty_shipped)                                                    AS qty_shipped_total,
            SUM(qty_ordered)                                                    AS qty_ordered_total,
            SUM(qty_backordered)                                                AS qty_backordered_total,
            SUM(qty_shipped)::NUMERIC / NULLIF(SUM(qty_ordered), 0)            AS fill_rate,
            SUM(qty_backordered)::NUMERIC / NULLIF(SUM(qty_ordered), 0)        AS backorder_rate,
            COUNT(*) FILTER (WHERE is_express_escalated = TRUE)                AS is_express_escalated_count
        FROM mart.fact_order_lines
        GROUP BY order_date
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_order_metrics_daily")
    print(f"agg_order_metrics_daily: {cur.fetchone()['n']} rows")


def agg_customer_scorecard(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_customer_scorecard (
            customer_id            TEXT PRIMARY KEY,
            company_name           TEXT,
            segment                TEXT,
            region                 TEXT,
            revenue_pln_total      NUMERIC(15,4),
            orders_count           INTEGER,
            lines_count            INTEGER,
            fill_rate              NUMERIC(8,4),
            backorder_rate         NUMERIC(8,4),
            return_rate            NUMERIC(8,4),
            payments_count         INTEGER,
            on_time_payment_rate   NUMERIC(8,4),
            latest_balance         NUMERIC(15,4),
            credit_utilisation_pct NUMERIC(8,4),
            credit_hold_count      INTEGER
        )
    """)
    cur.execute("TRUNCATE mart.agg_customer_scorecard RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_customer_scorecard (
            customer_id, company_name, segment, region,
            revenue_pln_total, orders_count, lines_count,
            fill_rate, backorder_rate, return_rate,
            payments_count, on_time_payment_rate, latest_balance,
            credit_utilisation_pct, credit_hold_count
        )
        WITH order_agg AS (
            SELECT
                f.customer_id,
                SUM(f.line_revenue_pln)                                             AS revenue_pln_total,
                COUNT(DISTINCT f.order_id)                                          AS orders_count,
                COUNT(*)                                                            AS lines_count,
                SUM(f.qty_shipped)::NUMERIC / NULLIF(SUM(f.qty_ordered), 0)        AS fill_rate,
                SUM(f.qty_backordered)::NUMERIC / NULLIF(SUM(f.qty_ordered), 0)    AS backorder_rate,
                SUM(f.qty_shipped)                                                  AS total_shipped
            FROM mart.fact_order_lines f
            GROUP BY f.customer_id
        ),
        return_agg AS (
            SELECT
                r.customer_id,
                COUNT(*) AS return_line_count
            FROM mart.fact_returns r
            GROUP BY r.customer_id
        ),
        payment_agg AS (
            SELECT
                p.customer_id,
                COUNT(*)                                                             AS payments_count,
                COUNT(*) FILTER (WHERE p.payment_behaviour = 'on_time')::NUMERIC
                    / NULLIF(COUNT(*), 0)                                            AS on_time_payment_rate,
                (ARRAY_AGG(p.balance_after ORDER BY p.payment_date DESC))[1]        AS latest_balance
            FROM mart.fact_payments p
            GROUP BY p.customer_id
        ),
        credit_agg AS (
            SELECT
                c.customer_id,
                (ARRAY_AGG(c.utilisation_pct ORDER BY c.event_date DESC))[1]        AS credit_utilisation_pct,
                COUNT(*) FILTER (WHERE c.event_subtype = 'credit_hold')             AS credit_hold_count
            FROM mart.fact_credit_events c
            GROUP BY c.customer_id
        )
        SELECT
            dc.customer_id,
            dc.company_name,
            dc.segment,
            dc.region,
            COALESCE(oa.revenue_pln_total, 0)                                       AS revenue_pln_total,
            COALESCE(oa.orders_count, 0)                                            AS orders_count,
            COALESCE(oa.lines_count, 0)                                             AS lines_count,
            oa.fill_rate,
            oa.backorder_rate,
            ra.return_line_count::NUMERIC / NULLIF(oa.total_shipped, 0)            AS return_rate,
            COALESCE(pa.payments_count, 0)                                          AS payments_count,
            pa.on_time_payment_rate,
            pa.latest_balance,
            ca.credit_utilisation_pct,
            COALESCE(ca.credit_hold_count, 0)                                       AS credit_hold_count
        FROM mart.dim_customer dc
        LEFT JOIN order_agg oa       ON oa.customer_id = dc.customer_id
        LEFT JOIN return_agg ra      ON ra.customer_id = dc.customer_id
        LEFT JOIN payment_agg pa     ON pa.customer_id = dc.customer_id
        LEFT JOIN credit_agg ca      ON ca.customer_id = dc.customer_id
        WHERE dc.is_current = TRUE
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_customer_scorecard")
    print(f"agg_customer_scorecard: {cur.fetchone()['n']} rows")


def agg_sku_performance(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_sku_performance (
            sku                     TEXT PRIMARY KEY,
            valve_type              TEXT,
            dn                      INT,
            material                TEXT,
            revenue_pln_total       NUMERIC(15,4),
            units_sold              INTEGER,
            units_ordered           INTEGER,
            units_backordered       INTEGER,
            units_returned          INTEGER,
            stockout_days           INTEGER,
            days_below_safety_stock INTEGER,
            is_active               BOOLEAN
        )
    """)
    cur.execute("TRUNCATE mart.agg_sku_performance RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_sku_performance (
            sku, valve_type, dn, material,
            revenue_pln_total, units_sold, units_ordered, units_backordered,
            units_returned, stockout_days, days_below_safety_stock, is_active
        )
        WITH order_agg AS (
            SELECT
                f.sku,
                SUM(f.line_revenue_pln)     AS revenue_pln_total,
                SUM(f.qty_shipped)          AS units_sold,
                SUM(f.qty_ordered)          AS units_ordered,
                SUM(f.qty_backordered)      AS units_backordered
            FROM mart.fact_order_lines f
            GROUP BY f.sku
        ),
        inv_agg AS (
            SELECT
                i.sku,
                COUNT(*) FILTER (WHERE i.stockout_flag = TRUE)       AS stockout_days,
                COUNT(*) FILTER (WHERE i.below_safety_stock = TRUE)  AS days_below_safety_stock
            FROM mart.fact_inventory_daily i
            GROUP BY i.sku
        ),
        return_agg AS (
            SELECT
                r.sku,
                SUM(r.qty_returned) AS units_returned
            FROM mart.fact_returns r
            GROUP BY r.sku
        )
        SELECT
            ds.sku,
            ds.valve_type,
            ds.dn,
            ds.material,
            COALESCE(oa.revenue_pln_total, 0)           AS revenue_pln_total,
            COALESCE(oa.units_sold, 0)                  AS units_sold,
            COALESCE(oa.units_ordered, 0)               AS units_ordered,
            COALESCE(oa.units_backordered, 0)           AS units_backordered,
            COALESCE(ra.units_returned, 0)              AS units_returned,
            COALESCE(ia.stockout_days, 0)               AS stockout_days,
            COALESCE(ia.days_below_safety_stock, 0)     AS days_below_safety_stock,
            ds.is_active
        FROM mart.dim_sku ds
        LEFT JOIN order_agg oa   ON oa.sku = ds.sku
        LEFT JOIN inv_agg ia     ON ia.sku = ds.sku
        LEFT JOIN return_agg ra  ON ra.sku = ds.sku
        WHERE ds.is_current = TRUE
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_sku_performance")
    print(f"agg_sku_performance: {cur.fetchone()['n']} rows")


def agg_inventory_health(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_inventory_health (
            sku                      TEXT,
            warehouse_code           TEXT,
            avg_on_hand              NUMERIC(12,2),
            min_on_hand              INTEGER,
            max_on_hand              INTEGER,
            stockout_days            INTEGER,
            days_below_safety_stock  INTEGER,
            days_below_reorder_point INTEGER,
            total_snapshot_days      INTEGER,
            PRIMARY KEY (sku, warehouse_code)
        )
    """)
    cur.execute("TRUNCATE mart.agg_inventory_health RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_inventory_health (
            sku, warehouse_code, avg_on_hand, min_on_hand, max_on_hand,
            stockout_days, days_below_safety_stock, days_below_reorder_point,
            total_snapshot_days
        )
        SELECT
            i.sku,
            dw.warehouse_code,
            AVG(i.qty_on_hand)::NUMERIC(12,2)                                   AS avg_on_hand,
            MIN(i.qty_on_hand)                                                   AS min_on_hand,
            MAX(i.qty_on_hand)                                                   AS max_on_hand,
            COUNT(*) FILTER (WHERE i.stockout_flag = TRUE)                      AS stockout_days,
            COUNT(*) FILTER (WHERE i.below_safety_stock = TRUE)                 AS days_below_safety_stock,
            COUNT(*) FILTER (WHERE i.qty_available < i.reorder_point)           AS days_below_reorder_point,
            COUNT(*)                                                             AS total_snapshot_days
        FROM mart.fact_inventory_daily i
        JOIN mart.dim_warehouse dw ON dw.dim_warehouse_id = i.dim_warehouse_id
        GROUP BY i.sku, dw.warehouse_code
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_inventory_health")
    print(f"agg_inventory_health: {cur.fetchone()['n']} rows")


def agg_carrier_scorecard(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_carrier_scorecard (
            carrier_code     TEXT PRIMARY KEY,
            carrier_name     TEXT,
            loads_count      INTEGER,
            avg_weight_kg    NUMERIC(12,4),
            total_weight_kg  NUMERIC(15,4),
            base_reliability NUMERIC(4,3),
            transit_days_min INTEGER,
            transit_days_max INTEGER,
            return_rate      NUMERIC(8,4)
        )
    """)
    cur.execute("TRUNCATE mart.agg_carrier_scorecard RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_carrier_scorecard (
            carrier_code, carrier_name, loads_count, avg_weight_kg, total_weight_kg,
            base_reliability, transit_days_min, transit_days_max, return_rate
        )
        WITH shipment_agg AS (
            SELECT
                fs.carrier_code,
                COUNT(*)            AS loads_count,
                AVG(fs.total_weight_kg) AS avg_weight_kg,
                SUM(fs.total_weight_kg) AS total_weight_kg
            FROM mart.fact_shipments fs
            GROUP BY fs.carrier_code
        ),
        return_agg AS (
            SELECT
                sr.carrier_code,
                COUNT(*) AS return_line_count
            FROM staging.returns sr
            WHERE sr.carrier_code IS NOT NULL
            GROUP BY sr.carrier_code
        ),
        shipped_lines AS (
            SELECT fs.carrier_code, COUNT(*) AS total_shipped_lines
            FROM mart.fact_shipments fs
            GROUP BY fs.carrier_code
        )
        SELECT
            dc.carrier_code,
            dc.name                                                                  AS carrier_name,
            COALESCE(sa.loads_count, 0)                                              AS loads_count,
            sa.avg_weight_kg,
            COALESCE(sa.total_weight_kg, 0)                                          AS total_weight_kg,
            dc.base_reliability,
            dc.transit_days_min,
            dc.transit_days_max,
            ra.return_line_count::NUMERIC / NULLIF(sl.total_shipped_lines, 0)       AS return_rate
        FROM mart.dim_carrier dc
        LEFT JOIN shipment_agg sa   ON sa.carrier_code = dc.carrier_code
        LEFT JOIN return_agg ra     ON ra.carrier_code = dc.carrier_code
        LEFT JOIN shipped_lines sl  ON sl.carrier_code = dc.carrier_code
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_carrier_scorecard")
    print(f"agg_carrier_scorecard: {cur.fetchone()['n']} rows")


def agg_demand_accuracy(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_demand_accuracy (
            product_group             TEXT,
            period_month              DATE,
            plan_qty                  NUMERIC(15,4),
            forecast_qty              NUMERIC(15,4),
            plan_confidence_low       NUMERIC(15,4),
            plan_confidence_high      NUMERIC(15,4),
            forecast_lower            NUMERIC(15,4),
            forecast_upper            NUMERIC(15,4),
            plan_vs_forecast_variance NUMERIC(15,4),
            PRIMARY KEY (product_group, period_month)
        )
    """)
    cur.execute("TRUNCATE mart.agg_demand_accuracy RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_demand_accuracy (
            product_group, period_month, plan_qty, forecast_qty,
            plan_confidence_low, plan_confidence_high,
            forecast_lower, forecast_upper, plan_vs_forecast_variance
        )
        WITH plan_agg AS (
            SELECT
                product_group,
                period_month,
                SUM(planned_qty)        AS plan_qty,
                SUM(confidence_low)     AS plan_confidence_low,
                SUM(confidence_high)    AS plan_confidence_high
            FROM staging.demand_plans
            GROUP BY product_group, period_month
        ),
        forecast_agg AS (
            SELECT
                product_group,
                period_month,
                SUM(forecast_qty)   AS forecast_qty,
                SUM(lower_bound)    AS forecast_lower,
                SUM(upper_bound)    AS forecast_upper
            FROM staging.demand_forecasts
            GROUP BY product_group, period_month
        )
        SELECT
            COALESCE(p.product_group, f.product_group)  AS product_group,
            COALESCE(p.period_month, f.period_month)    AS period_month,
            p.plan_qty,
            f.forecast_qty,
            p.plan_confidence_low,
            p.plan_confidence_high,
            f.forecast_lower,
            f.forecast_upper,
            p.plan_qty - f.forecast_qty                 AS plan_vs_forecast_variance
        FROM plan_agg p
        FULL OUTER JOIN forecast_agg f
            ON f.product_group = p.product_group
           AND f.period_month  = p.period_month
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_demand_accuracy")
    print(f"agg_demand_accuracy: {cur.fetchone()['n']} rows")


def agg_cash_flow_monthly(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_cash_flow_monthly (
            year                  INTEGER,
            month                 INTEGER,
            revenue_pln           NUMERIC(15,4),
            payments_received_pln NUMERIC(15,4),
            credit_exposure_pln   NUMERIC(15,4),
            PRIMARY KEY (year, month)
        )
    """)
    cur.execute("TRUNCATE mart.agg_cash_flow_monthly RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_cash_flow_monthly (
            year, month, revenue_pln, payments_received_pln, credit_exposure_pln
        )
        WITH revenue AS (
            SELECT
                EXTRACT(YEAR  FROM order_date)::INTEGER AS year,
                EXTRACT(MONTH FROM order_date)::INTEGER AS month,
                SUM(line_revenue_pln)                   AS revenue_pln
            FROM mart.fact_order_lines
            GROUP BY 1, 2
        ),
        payments AS (
            SELECT
                EXTRACT(YEAR  FROM payment_date)::INTEGER AS year,
                EXTRACT(MONTH FROM payment_date)::INTEGER AS month,
                SUM(amount_pln)                           AS payments_received_pln
            FROM mart.fact_payments
            GROUP BY 1, 2
        ),
        -- Latest exposure per customer per month, then summed across customers
        credit_latest AS (
            SELECT DISTINCT ON (customer_id,
                                EXTRACT(YEAR FROM event_date)::INTEGER,
                                EXTRACT(MONTH FROM event_date)::INTEGER)
                customer_id,
                EXTRACT(YEAR  FROM event_date)::INTEGER AS year,
                EXTRACT(MONTH FROM event_date)::INTEGER AS month,
                exposure
            FROM mart.fact_credit_events
            ORDER BY customer_id,
                     EXTRACT(YEAR FROM event_date)::INTEGER,
                     EXTRACT(MONTH FROM event_date)::INTEGER,
                     event_date DESC
        ),
        credit AS (
            SELECT year, month, SUM(exposure) AS credit_exposure_pln
            FROM credit_latest
            GROUP BY year, month
        ),
        all_months AS (
            SELECT year, month FROM revenue
            UNION
            SELECT year, month FROM payments
            UNION
            SELECT year, month FROM credit
        )
        SELECT
            am.year,
            am.month,
            r.revenue_pln,
            p.payments_received_pln,
            c.credit_exposure_pln
        FROM all_months am
        LEFT JOIN revenue  r ON r.year = am.year AND r.month = am.month
        LEFT JOIN payments p ON p.year = am.year AND p.month = am.month
        LEFT JOIN credit   c ON c.year = am.year AND c.month = am.month
        ORDER BY am.year, am.month
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_cash_flow_monthly")
    print(f"agg_cash_flow_monthly: {cur.fetchone()['n']} rows")


def agg_production_quality(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.agg_production_quality (
            sku                    TEXT,
            production_line        TEXT,
            year                   INTEGER,
            week_iso               INTEGER,
            qty_grade_a            INTEGER,
            qty_grade_b            INTEGER,
            total_qty              INTEGER,
            grade_a_pct            NUMERIC(8,4),
            grade_b_pct            NUMERIC(8,4),
            reclassification_count INTEGER,
            PRIMARY KEY (sku, production_line, year, week_iso)
        )
    """)
    cur.execute("TRUNCATE mart.agg_production_quality RESTART IDENTITY")
    cur.execute("""
        INSERT INTO mart.agg_production_quality (
            sku, production_line, year, week_iso,
            qty_grade_a, qty_grade_b, total_qty,
            grade_a_pct, grade_b_pct, reclassification_count
        )
        WITH prod_agg AS (
            SELECT
                fp.sku,
                COALESCE(fp.production_line, 'UNKNOWN') AS production_line,
                dd.year,
                dd.week_iso,
                SUM(fp.qty_completed) FILTER (WHERE fp.grade = 'A')    AS qty_grade_a,
                SUM(fp.qty_completed) FILTER (WHERE fp.grade = 'B')    AS qty_grade_b,
                SUM(fp.qty_completed)                                   AS total_qty
            FROM mart.fact_production fp
            JOIN mart.dim_date dd ON dd.date_id = fp.completion_date
            GROUP BY fp.sku, fp.production_line, dd.year, dd.week_iso
        ),
        reclassif_agg AS (
            SELECT
                sku,
                EXTRACT(YEAR FROM reclassification_date)::INTEGER   AS year,
                EXTRACT(WEEK FROM reclassification_date)::INTEGER   AS week_iso,
                COUNT(*)                                             AS reclassification_count
            FROM staging.production_reclassifications
            GROUP BY sku, 2, 3
        )
        SELECT
            pa.sku,
            pa.production_line,
            pa.year,
            pa.week_iso,
            COALESCE(pa.qty_grade_a, 0)                                             AS qty_grade_a,
            COALESCE(pa.qty_grade_b, 0)                                             AS qty_grade_b,
            pa.total_qty,
            COALESCE(pa.qty_grade_a, 0)::NUMERIC / NULLIF(pa.total_qty, 0)         AS grade_a_pct,
            COALESCE(pa.qty_grade_b, 0)::NUMERIC / NULLIF(pa.total_qty, 0)         AS grade_b_pct,
            COALESCE(ra.reclassification_count, 0)                                  AS reclassification_count
        FROM prod_agg pa
        LEFT JOIN reclassif_agg ra
            ON  ra.sku      = pa.sku
            AND ra.year     = pa.year
            AND ra.week_iso = pa.week_iso
    """)
    cur.execute("SELECT COUNT(*) AS n FROM mart.agg_production_quality")
    print(f"agg_production_quality: {cur.fetchone()['n']} rows")


def main():
    with connect() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                agg_order_metrics_daily(cur)
            with conn.transaction():
                agg_customer_scorecard(cur)
            with conn.transaction():
                agg_sku_performance(cur)
            with conn.transaction():
                agg_inventory_health(cur)
            with conn.transaction():
                agg_carrier_scorecard(cur)
            with conn.transaction():
                agg_demand_accuracy(cur)
            with conn.transaction():
                agg_cash_flow_monthly(cur)
            with conn.transaction():
                agg_production_quality(cur)

        print("\n--- Sanity checks ---")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM mart.agg_order_metrics_daily")
            print(f"agg_order_metrics_daily rows: {cur.fetchone()['n']}  (expected ~174)")
            cur.execute("SELECT COUNT(*) AS n FROM mart.agg_customer_scorecard")
            print(f"agg_customer_scorecard rows:  {cur.fetchone()['n']}  (expected ~60)")
            cur.execute("SELECT COUNT(*) AS n FROM mart.agg_sku_performance")
            print(f"agg_sku_performance rows:     {cur.fetchone()['n']}  (expected ~2500)")
            cur.execute("SELECT SUM(revenue_pln_total) AS s FROM mart.agg_customer_scorecard")
            print(f"agg_customer_scorecard total revenue_pln: {cur.fetchone()['s']}")


if __name__ == "__main__":
    main()
