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


DDL_DQ_REJECTS = """
CREATE TABLE IF NOT EXISTS staging.dq_rejects (
    reject_id   BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT,
    event_type  TEXT,
    reason      TEXT,
    payload     JSONB,
    rejected_at TIMESTAMPTZ DEFAULT now()
)
"""

DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS staging.orders (
    stg_id                  BIGSERIAL PRIMARY KEY,
    raw_event_id            BIGINT REFERENCES raw.events(id),
    ingested_at             TIMESTAMPTZ DEFAULT now(),
    _dq_flags               TEXT[],
    order_id                TEXT NOT NULL UNIQUE,
    customer_id             TEXT,
    status                  TEXT,
    currency                TEXT,
    priority                TEXT,
    order_timestamp         TIMESTAMPTZ,
    simulation_date         DATE,
    requested_delivery_date DATE,
    sales_channel           TEXT,
    incoterms               TEXT
)
"""

DDL_ORDER_LINES = """
CREATE TABLE IF NOT EXISTS staging.order_lines (
    stg_id              BIGSERIAL PRIMARY KEY,
    raw_event_id        BIGINT REFERENCES raw.events(id),
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    _dq_flags           TEXT[],
    order_id            TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    sku                 TEXT,
    unit_price          NUMERIC(15,4),
    line_status         TEXT,
    quantity_ordered    INTEGER,
    quantity_allocated  INTEGER,
    quantity_shipped    INTEGER,
    quantity_backordered INTEGER,
    UNIQUE (order_id, line_id)
)
"""

WATERMARK_SQL = "SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.orders"

FETCH_EVENTS_SQL = """
SELECT id, payload
FROM raw.events
WHERE event_type = 'customer_orders'
  AND id > %s
ORDER BY id
"""

UPSERT_ORDER_SQL = """
INSERT INTO staging.orders (
    raw_event_id, order_id, customer_id, status, currency, priority,
    order_timestamp, simulation_date, requested_delivery_date,
    sales_channel, incoterms, _dq_flags
) VALUES (
    %(raw_event_id)s, %(order_id)s, %(customer_id)s, %(status)s, %(currency)s, %(priority)s,
    %(order_timestamp)s, %(simulation_date)s, %(requested_delivery_date)s,
    %(sales_channel)s, %(incoterms)s, %(dq_flags)s
)
ON CONFLICT (order_id) DO UPDATE SET
    raw_event_id            = EXCLUDED.raw_event_id,
    ingested_at             = now(),
    _dq_flags               = EXCLUDED._dq_flags,
    customer_id             = EXCLUDED.customer_id,
    status                  = EXCLUDED.status,
    currency                = EXCLUDED.currency,
    priority                = EXCLUDED.priority,
    order_timestamp         = EXCLUDED.order_timestamp,
    simulation_date         = EXCLUDED.simulation_date,
    requested_delivery_date = EXCLUDED.requested_delivery_date,
    sales_channel           = EXCLUDED.sales_channel,
    incoterms               = EXCLUDED.incoterms
"""

UPSERT_LINE_SQL = """
INSERT INTO staging.order_lines (
    raw_event_id, order_id, line_id, sku, unit_price, line_status,
    quantity_ordered, quantity_allocated, quantity_shipped, quantity_backordered,
    _dq_flags
) VALUES (
    %(raw_event_id)s, %(order_id)s, %(line_id)s, %(sku)s, %(unit_price)s, %(line_status)s,
    %(quantity_ordered)s, %(quantity_allocated)s, %(quantity_shipped)s, %(quantity_backordered)s,
    %(dq_flags)s
)
ON CONFLICT (order_id, line_id) DO UPDATE SET
    raw_event_id         = EXCLUDED.raw_event_id,
    ingested_at          = now(),
    _dq_flags            = EXCLUDED._dq_flags,
    sku                  = EXCLUDED.sku,
    unit_price           = EXCLUDED.unit_price,
    line_status          = EXCLUDED.line_status,
    quantity_ordered     = EXCLUDED.quantity_ordered,
    quantity_allocated   = EXCLUDED.quantity_allocated,
    quantity_shipped     = EXCLUDED.quantity_shipped,
    quantity_backordered = EXCLUDED.quantity_backordered
"""

INSERT_REJECT_SQL = """
INSERT INTO staging.dq_rejects (raw_event_id, event_type, reason, payload)
VALUES (%s, %s, %s, %s)
"""


def run():
    with connect() as conn:
        conn.execute(DDL_DQ_REJECTS)
        conn.execute(DDL_ORDERS)
        conn.execute(DDL_ORDER_LINES)
        conn.commit()

        watermark = conn.execute(WATERMARK_SQL).fetchone()["coalesce"]
        rows = conn.execute(FETCH_EVENTS_SQL, (watermark,)).fetchall()

        orders_upserted = 0
        lines_upserted = 0
        rejected = 0

        with conn.transaction():
            for row in rows:
                raw_event_id = row["id"]
                p = row["payload"]

                order_id = p.get("order_id")
                customer_id = p.get("customer_id")

                if not order_id:
                    conn.execute(INSERT_REJECT_SQL, (
                        raw_event_id, "customer_orders", "null order_id",
                        psycopg.types.json.Jsonb(p),
                    ))
                    rejected += 1
                    continue

                if not customer_id:
                    conn.execute(INSERT_REJECT_SQL, (
                        raw_event_id, "customer_orders", "null customer_id",
                        psycopg.types.json.Jsonb(p),
                    ))
                    rejected += 1
                    continue

                dq_flags = []
                if p.get("currency") is None:
                    dq_flags.append("null_currency")
                if p.get("status") is None:
                    dq_flags.append("null_status")
                if p.get("priority") is None:
                    dq_flags.append("null_priority")

                conn.execute(UPSERT_ORDER_SQL, {
                    "raw_event_id": raw_event_id,
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "status": p.get("status"),
                    "currency": p.get("currency"),
                    "priority": p.get("priority"),
                    "order_timestamp": p.get("timestamp"),
                    "simulation_date": p.get("simulation_date"),
                    "requested_delivery_date": p.get("requested_delivery_date"),
                    "sales_channel": p.get("sales_channel"),
                    "incoterms": p.get("incoterms"),
                    "dq_flags": dq_flags if dq_flags else None,
                })
                orders_upserted += 1

                for line in p.get("lines", []):
                    conn.execute(UPSERT_LINE_SQL, {
                        "raw_event_id": raw_event_id,
                        "order_id": order_id,
                        "line_id": line.get("line_id"),
                        "sku": line.get("sku"),
                        "unit_price": line.get("unit_price"),
                        "line_status": line.get("line_status"),
                        "quantity_ordered": line.get("quantity_ordered"),
                        "quantity_allocated": line.get("quantity_allocated"),
                        "quantity_shipped": line.get("quantity_shipped"),
                        "quantity_backordered": line.get("quantity_backordered"),
                        "dq_flags": None,
                    })
                    lines_upserted += 1

        print(f"Processed : {len(rows)} raw events")
        print(f"Rejected  : {rejected}")
        print(f"Orders    : {orders_upserted} upserted")
        print(f"Lines     : {lines_upserted} upserted")


if __name__ == "__main__":
    run()
