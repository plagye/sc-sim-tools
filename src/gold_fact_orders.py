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


DDL = """
CREATE TABLE IF NOT EXISTS mart.fact_order_lines (
    fact_id             BIGSERIAL PRIMARY KEY,
    raw_order_event_id  BIGINT REFERENCES raw.events(id),
    raw_line_event_id   BIGINT REFERENCES raw.events(id),
    ingested_at         TIMESTAMPTZ DEFAULT now(),

    dim_date_id         DATE REFERENCES mart.dim_date(date_id),
    dim_customer_id     BIGINT REFERENCES mart.dim_customer(dim_customer_id),
    dim_sku_id          BIGINT REFERENCES mart.dim_sku(dim_sku_id),

    order_id            TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    customer_id         TEXT NOT NULL,

    order_date          DATE,
    requested_delivery_date DATE,
    order_status        TEXT,
    order_priority      TEXT,
    currency            TEXT,
    sales_channel       TEXT,
    incoterms           TEXT,

    qty_ordered         INTEGER,
    qty_allocated       INTEGER,
    qty_shipped         INTEGER,
    qty_backordered     INTEGER,

    unit_price_original NUMERIC(15,4),
    unit_price_pln      NUMERIC(15,4),
    line_revenue_pln    NUMERIC(15,4),

    is_backordered      BOOLEAN DEFAULT FALSE,
    is_express_escalated BOOLEAN DEFAULT FALSE,

    UNIQUE (order_id, line_id)
)
"""

UPSERT = """
INSERT INTO mart.fact_order_lines (
    raw_order_event_id,
    raw_line_event_id,
    dim_date_id,
    dim_customer_id,
    dim_sku_id,
    order_id,
    line_id,
    sku,
    customer_id,
    order_date,
    requested_delivery_date,
    order_status,
    order_priority,
    currency,
    sales_channel,
    incoterms,
    qty_ordered,
    qty_allocated,
    qty_shipped,
    qty_backordered,
    unit_price_original,
    unit_price_pln,
    line_revenue_pln,
    is_backordered,
    is_express_escalated
)
SELECT
    o.raw_event_id,
    ol.raw_event_id,
    COALESCE(o.simulation_date, o.order_timestamp::date),
    dc.dim_customer_id,
    ds.dim_sku_id,
    o.order_id,
    ol.line_id,
    ol.sku,
    o.customer_id,
    o.order_timestamp::date,
    o.requested_delivery_date,
    o.status,
    o.priority,
    o.currency,
    o.sales_channel,
    o.incoterms,
    ol.quantity_ordered,
    ol.quantity_allocated,
    ol.quantity_shipped,
    ol.quantity_backordered,
    ol.unit_price,
    CASE
        WHEN o.currency = 'PLN' THEN ol.unit_price
        ELSE ol.unit_price * COALESCE(
            (SELECT rate FROM staging.exchange_rates
             WHERE currency_pair = o.currency || '/PLN'
               AND rate_date <= COALESCE(o.simulation_date, o.order_timestamp::date)
             ORDER BY rate_date DESC LIMIT 1),
            1.0
        )
    END,
    ol.quantity_shipped * CASE
        WHEN o.currency = 'PLN' THEN ol.unit_price
        ELSE ol.unit_price * COALESCE(
            (SELECT rate FROM staging.exchange_rates
             WHERE currency_pair = o.currency || '/PLN'
               AND rate_date <= COALESCE(o.simulation_date, o.order_timestamp::date)
             ORDER BY rate_date DESC LIMIT 1),
            1.0
        )
    END,
    (ol.quantity_backordered > 0),
    EXISTS (
        SELECT 1 FROM staging.backorder_events be
        WHERE be.order_id = o.order_id
          AND be.order_line_id = ol.line_id
          AND be.escalated_to_express = TRUE
    )
FROM staging.order_lines ol
JOIN staging.orders o ON o.order_id = ol.order_id
JOIN LATERAL (
    SELECT dim_customer_id FROM mart.dim_customer
    WHERE customer_id = o.customer_id
      AND effective_from <= COALESCE(o.simulation_date, o.order_timestamp::date)
      AND (effective_to IS NULL OR effective_to >= COALESCE(o.simulation_date, o.order_timestamp::date))
    ORDER BY effective_from DESC LIMIT 1
) dc ON TRUE
JOIN LATERAL (
    SELECT dim_sku_id FROM mart.dim_sku
    WHERE sku = ol.sku
      AND effective_from <= COALESCE(o.simulation_date, o.order_timestamp::date)
      AND (effective_to IS NULL OR effective_to >= COALESCE(o.simulation_date, o.order_timestamp::date))
    ORDER BY effective_from DESC LIMIT 1
) ds ON TRUE
ON CONFLICT (order_id, line_id) DO UPDATE SET
    qty_ordered          = EXCLUDED.qty_ordered,
    qty_allocated        = EXCLUDED.qty_allocated,
    qty_shipped          = EXCLUDED.qty_shipped,
    qty_backordered      = EXCLUDED.qty_backordered,
    order_status         = EXCLUDED.order_status,
    unit_price_pln       = EXCLUDED.unit_price_pln,
    line_revenue_pln     = EXCLUDED.line_revenue_pln,
    is_backordered       = EXCLUDED.is_backordered,
    is_express_escalated = EXCLUDED.is_express_escalated,
    ingested_at          = now()
"""


def run():
    with connect() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute(DDL)
                cur.execute(UPSERT)
                cur.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines")
                total = cur.fetchone()["n"]
                print(f"fact_order_lines: {total} rows upserted")

        print("\n--- Verification ---")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines")
            print(f"Total rows: {cur.fetchone()['n']}")

            cur.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines WHERE dim_customer_id IS NULL")
            print(f"NULL dim_customer_id: {cur.fetchone()['n']}")

            cur.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines WHERE unit_price_pln IS NULL")
            print(f"NULL unit_price_pln: {cur.fetchone()['n']}")

            cur.execute("""
                SELECT currency, COUNT(*) AS n, AVG(unit_price_pln)::NUMERIC(12,4) AS avg_price_pln
                FROM mart.fact_order_lines
                GROUP BY currency
                ORDER BY currency
            """)
            for r in cur.fetchall():
                print(f"  currency={r['currency']}  rows={r['n']}  avg_unit_price_pln={r['avg_price_pln']}")

            cur.execute("SELECT COUNT(*) AS n FROM mart.fact_order_lines WHERE is_express_escalated")
            print(f"is_express_escalated=TRUE: {cur.fetchone()['n']}")


if __name__ == "__main__":
    run()
