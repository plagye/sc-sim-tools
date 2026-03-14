import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv("/home/coder/sc-sim-tools/.env")


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


def load_fact_payments(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_payments (
            fact_id           BIGSERIAL PRIMARY KEY,
            raw_event_id      BIGINT REFERENCES raw.events(id),
            ingested_at       TIMESTAMPTZ DEFAULT now(),
            dim_date_id       DATE REFERENCES mart.dim_date(date_id),
            dim_customer_id   BIGINT REFERENCES mart.dim_customer(dim_customer_id),
            event_id          TEXT NOT NULL UNIQUE,
            payment_date      DATE,
            customer_id       TEXT,
            amount_pln        NUMERIC(15,4),
            payment_behaviour TEXT,
            balance_before    NUMERIC(15,4),
            balance_after     NUMERIC(15,4)
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_payments (
            raw_event_id, dim_date_id, dim_customer_id,
            event_id, payment_date, customer_id,
            amount_pln, payment_behaviour, balance_before, balance_after
        )
        SELECT
            p.raw_event_id,
            CASE WHEN p.payment_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN p.payment_date ELSE NULL END,
            dc.dim_customer_id,
            p.event_id, p.payment_date, p.customer_id,
            p.amount, p.payment_behaviour, p.balance_before, p.balance_after
        FROM staging.payments p
        JOIN LATERAL (
            SELECT dim_customer_id FROM mart.dim_customer
            WHERE customer_id = p.customer_id
              AND effective_from <= p.payment_date
              AND (effective_to IS NULL OR effective_to >= p.payment_date)
            ORDER BY effective_from DESC LIMIT 1
        ) dc ON TRUE
        ON CONFLICT (event_id) DO UPDATE SET
            amount_pln        = EXCLUDED.amount_pln,
            payment_behaviour = EXCLUDED.payment_behaviour,
            balance_before    = EXCLUDED.balance_before,
            balance_after     = EXCLUDED.balance_after,
            ingested_at       = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_payments")
    print(f"fact_payments: {cur.fetchone()['n']} rows")


def load_fact_credit_events(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_credit_events (
            fact_id           BIGSERIAL PRIMARY KEY,
            raw_event_id      BIGINT REFERENCES raw.events(id),
            ingested_at       TIMESTAMPTZ DEFAULT now(),
            dim_date_id       DATE REFERENCES mart.dim_date(date_id),
            dim_customer_id   BIGINT REFERENCES mart.dim_customer(dim_customer_id),
            event_id          TEXT NOT NULL UNIQUE,
            event_date        DATE,
            customer_id       TEXT,
            event_subtype     TEXT,
            customer_balance  NUMERIC(15,4),
            open_order_value  NUMERIC(15,4),
            credit_limit      NUMERIC(15,4),
            exposure          NUMERIC(15,4),
            utilisation_pct   NUMERIC(8,4) GENERATED ALWAYS AS (
                CASE WHEN credit_limit > 0 THEN exposure / credit_limit ELSE NULL END
            ) STORED
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_credit_events (
            raw_event_id, dim_date_id, dim_customer_id,
            event_id, event_date, customer_id, event_subtype,
            customer_balance, open_order_value, credit_limit, exposure
        )
        SELECT
            ce.raw_event_id,
            CASE WHEN ce.event_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN ce.event_date ELSE NULL END,
            dc.dim_customer_id,
            ce.event_id, ce.event_date, ce.customer_id, ce.event_subtype,
            ce.customer_balance, ce.open_order_value, ce.credit_limit, ce.exposure
        FROM staging.credit_events ce
        JOIN LATERAL (
            SELECT dim_customer_id FROM mart.dim_customer
            WHERE customer_id = ce.customer_id
              AND effective_from <= ce.event_date
              AND (effective_to IS NULL OR effective_to >= ce.event_date)
            ORDER BY effective_from DESC LIMIT 1
        ) dc ON TRUE
        ON CONFLICT (event_id) DO UPDATE SET
            customer_balance = EXCLUDED.customer_balance,
            open_order_value = EXCLUDED.open_order_value,
            credit_limit     = EXCLUDED.credit_limit,
            exposure         = EXCLUDED.exposure,
            ingested_at      = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_credit_events")
    print(f"fact_credit_events: {cur.fetchone()['n']} rows")


def load_fact_shipments(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_shipments (
            fact_id             BIGSERIAL PRIMARY KEY,
            raw_event_id        BIGINT REFERENCES raw.events(id),
            ingested_at         TIMESTAMPTZ DEFAULT now(),
            dim_date_id         DATE REFERENCES mart.dim_date(date_id),
            dim_carrier_id      BIGINT REFERENCES mart.dim_carrier(dim_carrier_id),
            dim_warehouse_id    BIGINT REFERENCES mart.dim_warehouse(dim_warehouse_id),
            load_id             TEXT NOT NULL UNIQUE,
            load_date           DATE,
            carrier_code        TEXT,
            source_warehouse_id TEXT,
            destination_region  TEXT,
            status              TEXT,
            total_weight_kg     NUMERIC(12,4),
            shipment_ids        JSONB,
            order_ids           JSONB,
            customer_ids        JSONB
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_shipments (
            raw_event_id, dim_date_id, dim_carrier_id, dim_warehouse_id,
            load_id, load_date, carrier_code, source_warehouse_id,
            destination_region, status, total_weight_kg,
            shipment_ids, order_ids, customer_ids
        )
        SELECT
            l.raw_event_id,
            CASE WHEN l.load_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN l.load_date ELSE NULL END,
            dc.dim_carrier_id,
            dw.dim_warehouse_id,
            l.load_id, l.load_date, l.carrier_code, l.source_warehouse_id,
            l.destination_region, l.status, l.total_weight_kg,
            to_jsonb(l.shipment_ids), to_jsonb(l.order_ids), to_jsonb(l.customer_ids)
        FROM staging.loads l
        JOIN LATERAL (
            SELECT dim_carrier_id FROM mart.dim_carrier
            WHERE carrier_code = l.carrier_code LIMIT 1
        ) dc ON TRUE
        JOIN LATERAL (
            SELECT dim_warehouse_id FROM mart.dim_warehouse
            WHERE warehouse_code = l.source_warehouse_id LIMIT 1
        ) dw ON TRUE
        ON CONFLICT (load_id) DO UPDATE SET
            status          = EXCLUDED.status,
            total_weight_kg = EXCLUDED.total_weight_kg,
            ingested_at     = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_shipments")
    print(f"fact_shipments: {cur.fetchone()['n']} rows")


def load_fact_returns(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_returns (
            fact_id          BIGSERIAL PRIMARY KEY,
            raw_event_id     BIGINT REFERENCES raw.events(id),
            ingested_at      TIMESTAMPTZ DEFAULT now(),
            dim_date_id      DATE REFERENCES mart.dim_date(date_id),
            dim_customer_id  BIGINT REFERENCES mart.dim_customer(dim_customer_id),
            dim_sku_id       BIGINT REFERENCES mart.dim_sku(dim_sku_id),
            dim_carrier_id   BIGINT REFERENCES mart.dim_carrier(dim_carrier_id),
            rma_id           TEXT NOT NULL,
            line_id          TEXT NOT NULL,
            order_id         TEXT,
            customer_id      TEXT,
            sku              TEXT,
            event_date       DATE,
            event_subtype    TEXT,
            qty_returned     INTEGER,
            qty_accepted     INTEGER,
            return_reason    TEXT,
            resolution       TEXT,
            UNIQUE (rma_id, line_id)
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_returns (
            raw_event_id, dim_date_id, dim_customer_id, dim_sku_id, dim_carrier_id,
            rma_id, line_id, order_id, customer_id, sku,
            event_date, event_subtype, qty_returned, qty_accepted,
            return_reason, resolution
        )
        SELECT
            rr.raw_event_id,
            CASE WHEN rr.event_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN rr.event_date ELSE NULL END,
            dc.dim_customer_id,
            ds.dim_sku_id,
            dcarr.dim_carrier_id,
            rl.rma_id, rl.line_id, rr.order_id, rr.customer_id, rl.sku,
            rr.event_date, rr.event_subtype,
            rl.quantity_returned, rl.quantity_accepted,
            rl.return_reason, rl.resolution
        FROM staging.return_lines rl
        JOIN (
            SELECT DISTINCT ON (rma_id) rma_id, event_date, event_subtype,
                   order_id, customer_id, carrier_code, raw_event_id
            FROM staging.returns
            WHERE event_subtype = 'return_requested'
            ORDER BY rma_id, event_date ASC
        ) rr ON rr.rma_id = rl.rma_id
        JOIN LATERAL (
            SELECT dim_customer_id FROM mart.dim_customer
            WHERE customer_id = rr.customer_id
              AND effective_from <= rr.event_date
              AND (effective_to IS NULL OR effective_to >= rr.event_date)
            ORDER BY effective_from DESC LIMIT 1
        ) dc ON TRUE
        JOIN LATERAL (
            SELECT dim_sku_id FROM mart.dim_sku
            WHERE sku = rl.sku
              AND effective_from <= rr.event_date
              AND (effective_to IS NULL OR effective_to >= rr.event_date)
            ORDER BY effective_from DESC LIMIT 1
        ) ds ON TRUE
        JOIN LATERAL (
            SELECT dim_carrier_id FROM mart.dim_carrier
            WHERE carrier_code = rr.carrier_code LIMIT 1
        ) dcarr ON TRUE
        ON CONFLICT (rma_id, line_id) DO UPDATE SET
            qty_accepted  = EXCLUDED.qty_accepted,
            resolution    = EXCLUDED.resolution,
            event_subtype = EXCLUDED.event_subtype,
            ingested_at   = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_returns")
    print(f"fact_returns: {cur.fetchone()['n']} rows")


def run():
    with connect() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                load_fact_payments(cur)
            with conn.transaction():
                load_fact_credit_events(cur)
            with conn.transaction():
                load_fact_shipments(cur)
            with conn.transaction():
                load_fact_returns(cur)


if __name__ == "__main__":
    run()
