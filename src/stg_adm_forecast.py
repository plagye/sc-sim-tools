import os
import psycopg
from dotenv import load_dotenv

load_dotenv()


def connect():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"],
        sslmode=os.environ["DB_SSL"],
    )


DDL_DEMAND_PLANS = """
CREATE TABLE IF NOT EXISTS staging.demand_plans (
    stg_id           BIGSERIAL PRIMARY KEY,
    raw_event_id     BIGINT REFERENCES raw.events(id),
    ingested_at      TIMESTAMPTZ DEFAULT now(),
    _dq_flags        TEXT[],
    plan_month       DATE NOT NULL,
    product_group    TEXT NOT NULL,
    customer_segment TEXT NOT NULL,
    period_month     DATE NOT NULL,
    planned_qty      NUMERIC(15,4),
    confidence_low   NUMERIC(15,4),
    confidence_high  NUMERIC(15,4),
    UNIQUE (plan_month, product_group, customer_segment, period_month)
)
"""

DDL_INVENTORY_TARGETS = """
CREATE TABLE IF NOT EXISTS staging.inventory_targets (
    stg_id          BIGSERIAL PRIMARY KEY,
    raw_event_id    BIGINT REFERENCES raw.events(id),
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    _dq_flags       TEXT[],
    target_month    DATE NOT NULL,
    sku             TEXT NOT NULL,
    warehouse_id    TEXT NOT NULL,
    safety_stock    INTEGER,
    reorder_point   INTEGER,
    target_stock    INTEGER,
    review_trigger  TEXT,
    UNIQUE (target_month, sku, warehouse_id)
)
"""

DDL_DEMAND_SIGNALS = """
CREATE TABLE IF NOT EXISTS staging.demand_signals (
    stg_id         BIGSERIAL PRIMARY KEY,
    raw_event_id   BIGINT REFERENCES raw.events(id),
    ingested_at    TIMESTAMPTZ DEFAULT now(),
    _dq_flags      TEXT[],
    signal_id      TEXT NOT NULL UNIQUE,
    signal_date    DATE,
    signal_source  TEXT,
    customer_id    TEXT,
    product_group  TEXT,
    signal_type    TEXT,
    magnitude      TEXT,
    horizon_weeks  INTEGER,
    confidence     NUMERIC(6,4)
)
"""

DDL_DEMAND_FORECASTS = """
CREATE TABLE IF NOT EXISTS staging.demand_forecasts (
    stg_id         BIGSERIAL PRIMARY KEY,
    raw_event_id   BIGINT REFERENCES raw.events(id),
    ingested_at    TIMESTAMPTZ DEFAULT now(),
    _dq_flags      TEXT[],
    forecast_month DATE NOT NULL,
    product_group  TEXT NOT NULL,
    period_month   DATE NOT NULL,
    forecast_qty   NUMERIC(15,4),
    lower_bound    NUMERIC(15,4),
    upper_bound    NUMERIC(15,4),
    model_version  TEXT,
    UNIQUE (forecast_month, product_group, period_month)
)
"""


def load_demand_plans(conn):
    conn.execute(DDL_DEMAND_PLANS)

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.demand_plans"
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'demand_plans' AND id > %s
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.demand_plans
            (raw_event_id, plan_month, product_group, customer_segment,
             period_month, planned_qty, confidence_low, confidence_high, _dq_flags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (plan_month, product_group, customer_segment, period_month)
        DO UPDATE SET
            raw_event_id    = EXCLUDED.raw_event_id,
            planned_qty     = EXCLUDED.planned_qty,
            confidence_low  = EXCLUDED.confidence_low,
            confidence_high = EXCLUDED.confidence_high,
            ingested_at     = now(),
            _dq_flags       = EXCLUDED._dq_flags
    """

    inserted = 0
    with conn.cursor() as cur:
        for raw_id, event_date, payload in rows:
            plan_month = event_date.replace(day=1)
            for line in payload.get("plan_lines", []):
                product_group = line.get("product_group")
                customer_segment = line.get("customer_segment")
                for period in line.get("periods", []):
                    period_month_str = period.get("month")
                    period_month = (
                        (lambda y, m: __import__("datetime").date(int(y), int(m), 1))(
                            *period_month_str.split("-")
                        )
                        if period_month_str
                        else None
                    )
                    cur.execute(
                        upsert_sql,
                        (
                            raw_id,
                            plan_month,
                            product_group,
                            customer_segment,
                            period_month,
                            period.get("planned_quantity"),
                            period.get("confidence_low"),
                            period.get("confidence_high"),
                            None,
                        ),
                    )
                    inserted += 1

    conn.commit()
    return inserted


def load_inventory_targets(conn):
    conn.execute(DDL_INVENTORY_TARGETS)

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.inventory_targets"
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'inventory_targets' AND id > %s
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.inventory_targets
            (raw_event_id, target_month, sku, warehouse_id,
             safety_stock, reorder_point, target_stock, review_trigger, _dq_flags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (target_month, sku, warehouse_id)
        DO UPDATE SET
            raw_event_id   = EXCLUDED.raw_event_id,
            safety_stock   = EXCLUDED.safety_stock,
            reorder_point  = EXCLUDED.reorder_point,
            target_stock   = EXCLUDED.target_stock,
            review_trigger = EXCLUDED.review_trigger,
            ingested_at    = now(),
            _dq_flags      = EXCLUDED._dq_flags
    """

    inserted = 0
    with conn.cursor() as cur:
        for raw_id, event_date, payload in rows:
            target_month = event_date.replace(day=1)
            cur.execute(
                upsert_sql,
                (
                    raw_id,
                    target_month,
                    payload.get("sku"),
                    payload.get("warehouse"),
                    payload.get("safety_stock"),
                    payload.get("reorder_point"),
                    payload.get("target_stock"),
                    payload.get("review_trigger"),
                    None,
                ),
            )
            inserted += 1

    conn.commit()
    return inserted


def load_demand_signals(conn):
    conn.execute(DDL_DEMAND_SIGNALS)

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.demand_signals"
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'demand_signals' AND id > %s
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.demand_signals
            (raw_event_id, signal_id, signal_date, signal_source, customer_id,
             product_group, signal_type, magnitude, horizon_weeks, confidence, _dq_flags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (signal_id)
        DO UPDATE SET
            raw_event_id  = EXCLUDED.raw_event_id,
            signal_date   = EXCLUDED.signal_date,
            signal_source = EXCLUDED.signal_source,
            customer_id   = EXCLUDED.customer_id,
            product_group = EXCLUDED.product_group,
            signal_type   = EXCLUDED.signal_type,
            magnitude     = EXCLUDED.magnitude,
            horizon_weeks = EXCLUDED.horizon_weeks,
            confidence    = EXCLUDED.confidence,
            ingested_at   = now(),
            _dq_flags     = EXCLUDED._dq_flags
    """

    inserted = 0
    with conn.cursor() as cur:
        for raw_id, event_date, payload in rows:
            cur.execute(
                upsert_sql,
                (
                    raw_id,
                    payload.get("signal_id"),
                    event_date,
                    payload.get("signal_source"),
                    payload.get("customer_id"),
                    payload.get("product_group"),
                    payload.get("signal_type"),
                    payload.get("magnitude"),
                    payload.get("horizon_weeks"),
                    payload.get("confidence"),
                    None,
                ),
            )
            inserted += 1

    conn.commit()
    return inserted


def load_demand_forecasts(conn):
    conn.execute(DDL_DEMAND_FORECASTS)

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.demand_forecasts"
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'demand_forecasts' AND id > %s
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.demand_forecasts
            (raw_event_id, forecast_month, product_group, period_month,
             forecast_qty, lower_bound, upper_bound, model_version, _dq_flags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (forecast_month, product_group, period_month)
        DO UPDATE SET
            raw_event_id  = EXCLUDED.raw_event_id,
            forecast_qty  = EXCLUDED.forecast_qty,
            lower_bound   = EXCLUDED.lower_bound,
            upper_bound   = EXCLUDED.upper_bound,
            model_version = EXCLUDED.model_version,
            ingested_at   = now(),
            _dq_flags     = EXCLUDED._dq_flags
    """

    inserted = 0
    with conn.cursor() as cur:
        for raw_id, event_date, payload in rows:
            forecast_month = event_date.replace(day=1)
            model_version = payload.get("model_version")
            for line in payload.get("lines", []):
                product_group = line.get("sku_group")
                for period in line.get("periods", []):
                    period_month_str = period.get("month")
                    period_month = (
                        (lambda y, m: __import__("datetime").date(int(y), int(m), 1))(
                            *period_month_str.split("-")
                        )
                        if period_month_str
                        else None
                    )
                    cur.execute(
                        upsert_sql,
                        (
                            raw_id,
                            forecast_month,
                            product_group,
                            period_month,
                            period.get("forecast_quantity"),
                            period.get("lower_bound"),
                            period.get("upper_bound"),
                            model_version,
                            None,
                        ),
                    )
                    inserted += 1

    conn.commit()
    return inserted


if __name__ == "__main__":
    with connect() as conn:
        n = load_demand_plans(conn)
        print(f"staging.demand_plans:      {n:>6} rows upserted")

    with connect() as conn:
        n = load_inventory_targets(conn)
        print(f"staging.inventory_targets: {n:>6} rows upserted")

    with connect() as conn:
        n = load_demand_signals(conn)
        print(f"staging.demand_signals:    {n:>6} rows upserted")

    with connect() as conn:
        n = load_demand_forecasts(conn)
        print(f"staging.demand_forecasts:  {n:>6} rows upserted")

    with connect() as conn:
        for tbl in ["demand_plans", "inventory_targets", "demand_signals", "demand_forecasts"]:
            cnt = conn.execute(f"SELECT COUNT(*) FROM staging.{tbl}").fetchone()[0]
            print(f"staging.{tbl} total rows: {cnt}")
