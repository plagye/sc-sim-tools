from db import connect


DDL_LOADS = """
CREATE TABLE IF NOT EXISTS staging.loads (
    stg_id              BIGSERIAL PRIMARY KEY,
    raw_event_id        BIGINT REFERENCES raw.events(id),
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    _dq_flags           TEXT[],
    load_id             TEXT NOT NULL,
    sync_window         TEXT,
    load_date           DATE,
    carrier_code        TEXT,
    source_warehouse_id TEXT,
    destination_region  TEXT,
    status              TEXT,
    priority            TEXT,
    total_weight_kg     NUMERIC(12,4),
    weight_unit_original TEXT,
    shipment_ids        JSONB,
    order_ids           JSONB,
    customer_ids        JSONB,
    CONSTRAINT loads_load_id_uq UNIQUE (load_id)
)
"""

DDL_CARRIER_EVENTS = """
CREATE TABLE IF NOT EXISTS staging.carrier_events (
    stg_id          BIGSERIAL PRIMARY KEY,
    raw_event_id    BIGINT REFERENCES raw.events(id),
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    _dq_flags       TEXT[],
    event_id        TEXT NOT NULL,
    event_date      DATE,
    carrier_code    TEXT,
    event_subtype   TEXT,
    region          TEXT,
    severity        TEXT,
    duration_days   INTEGER,
    end_date        DATE,
    rate_delta_pct  NUMERIC(8,4),
    CONSTRAINT carrier_events_event_id_uq UNIQUE (event_id)
)
"""

DDL_RETURNS = """
CREATE TABLE IF NOT EXISTS staging.returns (
    stg_id          BIGSERIAL PRIMARY KEY,
    raw_event_id    BIGINT REFERENCES raw.events(id),
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    _dq_flags       TEXT[],
    event_id        TEXT NOT NULL,
    event_date      DATE,
    rma_id          TEXT,
    event_subtype   TEXT,
    order_id        TEXT,
    customer_id     TEXT,
    carrier_code    TEXT,
    status          TEXT,
    CONSTRAINT returns_event_id_uq UNIQUE (event_id)
)
"""

DDL_RETURN_LINES = """
CREATE TABLE IF NOT EXISTS staging.return_lines (
    stg_id              BIGSERIAL PRIMARY KEY,
    raw_event_id        BIGINT REFERENCES raw.events(id),
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    _dq_flags           TEXT[],
    rma_id              TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    sku                 TEXT,
    quantity_returned   INTEGER,
    quantity_accepted   INTEGER,
    return_reason       TEXT,
    resolution          TEXT,
    CONSTRAINT return_lines_rma_line_uq UNIQUE (rma_id, line_id)
)
"""


def load_loads(conn):
    conn.execute(DDL_LOADS)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS w FROM staging.loads"
    ).fetchone()["w"]

    rows = conn.execute(
        """
        SELECT id, event_date, event_type, payload
        FROM raw.events
        WHERE event_type IN ('loads_08', 'loads_14', 'loads_20')
          AND id > %s
        ORDER BY id
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.loads (
            raw_event_id, _dq_flags, load_id, sync_window, load_date,
            carrier_code, source_warehouse_id, destination_region, status, priority,
            total_weight_kg, weight_unit_original, shipment_ids, order_ids, customer_ids
        ) VALUES (
            %(raw_event_id)s, %(dq_flags)s, %(load_id)s, %(sync_window)s, %(load_date)s,
            %(carrier_code)s, %(source_warehouse_id)s, %(destination_region)s, %(status)s, %(priority)s,
            %(total_weight_kg)s, %(weight_unit_original)s, %(shipment_ids)s, %(order_ids)s, %(customer_ids)s
        )
        ON CONFLICT (load_id) DO UPDATE SET
            raw_event_id        = EXCLUDED.raw_event_id,
            _dq_flags           = EXCLUDED._dq_flags,
            sync_window         = EXCLUDED.sync_window,
            load_date           = EXCLUDED.load_date,
            carrier_code        = EXCLUDED.carrier_code,
            source_warehouse_id = EXCLUDED.source_warehouse_id,
            destination_region  = EXCLUDED.destination_region,
            status              = EXCLUDED.status,
            priority            = EXCLUDED.priority,
            total_weight_kg     = EXCLUDED.total_weight_kg,
            weight_unit_original= EXCLUDED.weight_unit_original,
            shipment_ids        = EXCLUDED.shipment_ids,
            order_ids           = EXCLUDED.order_ids,
            customer_ids        = EXCLUDED.customer_ids
        WHERE EXCLUDED.sync_window > staging.loads.sync_window
    """

    import json as _json

    count = 0
    for row in rows:
        p = row["payload"]
        dq_flags = []
        weight_unit = p.get("weight_unit")
        raw_weight = p.get("total_weight_reported")

        if weight_unit == "lbs":
            weight_kg = raw_weight / 2.20462 if raw_weight is not None else None
            dq_flags.append("weight_converted_lbs_to_kg")
        else:
            weight_kg = raw_weight

        if not p.get("load_id"):
            continue

        sync_window = p.get("sync_window") or row["event_type"].split("_")[-1]

        conn.execute(upsert_sql, {
            "raw_event_id": row["id"],
            "dq_flags": dq_flags or None,
            "load_id": p.get("load_id"),
            "sync_window": sync_window,
            "load_date": row["event_date"],
            "carrier_code": p.get("carrier_code"),
            "source_warehouse_id": p.get("source_warehouse_id"),
            "destination_region": p.get("destination_region"),
            "status": p.get("status"),
            "priority": p.get("priority"),
            "total_weight_kg": weight_kg,
            "weight_unit_original": weight_unit,
            "shipment_ids": _json.dumps(p.get("shipment_ids")),
            "order_ids": _json.dumps(p.get("order_ids")),
            "customer_ids": _json.dumps(p.get("customer_ids")),
        })
        count += 1

    conn.commit()
    print(f"staging.loads: processed {count} raw events")
    total = conn.execute("SELECT COUNT(*) AS n FROM staging.loads").fetchone()["n"]
    print(f"staging.loads: {total} unique loads in table")


def load_carrier_events(conn):
    conn.execute(DDL_CARRIER_EVENTS)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS w FROM staging.carrier_events"
    ).fetchone()["w"]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'carrier_events'
          AND id > %s
        ORDER BY id
        """,
        (watermark,),
    ).fetchall()

    upsert_sql = """
        INSERT INTO staging.carrier_events (
            raw_event_id, _dq_flags, event_id, event_date, carrier_code,
            event_subtype, region, severity, duration_days, end_date, rate_delta_pct
        ) VALUES (
            %(raw_event_id)s, %(dq_flags)s, %(event_id)s, %(event_date)s, %(carrier_code)s,
            %(event_subtype)s, %(region)s, %(severity)s, %(duration_days)s, %(end_date)s, %(rate_delta_pct)s
        )
        ON CONFLICT (event_id) DO UPDATE SET
            raw_event_id  = EXCLUDED.raw_event_id,
            _dq_flags     = EXCLUDED._dq_flags,
            event_date    = EXCLUDED.event_date,
            carrier_code  = EXCLUDED.carrier_code,
            event_subtype = EXCLUDED.event_subtype,
            region        = EXCLUDED.region,
            severity      = EXCLUDED.severity,
            duration_days = EXCLUDED.duration_days,
            end_date      = EXCLUDED.end_date,
            rate_delta_pct= EXCLUDED.rate_delta_pct
    """

    count = 0
    for row in rows:
        p = row["payload"]
        end_date_raw = p.get("end_date")
        duration_raw = p.get("duration_days")
        rate_raw = p.get("rate_delta_pct")

        conn.execute(upsert_sql, {
            "raw_event_id": row["id"],
            "dq_flags": None,
            "event_id": p.get("event_id"),
            "event_date": row["event_date"],
            "carrier_code": p.get("carrier_code"),
            "event_subtype": p.get("event_subtype"),
            "region": p.get("affected_region"),
            "severity": p.get("impact_severity"),
            "duration_days": int(duration_raw) if duration_raw is not None else None,
            "end_date": end_date_raw if end_date_raw else None,
            "rate_delta_pct": rate_raw if rate_raw is not None else None,
        })
        count += 1

    conn.commit()
    print(f"staging.carrier_events: processed {count} raw events")
    total = conn.execute("SELECT COUNT(*) AS n FROM staging.carrier_events").fetchone()["n"]
    print(f"staging.carrier_events: {total} rows in table")


def load_returns(conn):
    conn.execute(DDL_RETURNS)
    conn.execute(DDL_RETURN_LINES)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS w FROM staging.returns"
    ).fetchone()["w"]

    rows = conn.execute(
        """
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'returns'
          AND id > %s
        ORDER BY id
        """,
        (watermark,),
    ).fetchall()

    upsert_header_sql = """
        INSERT INTO staging.returns (
            raw_event_id, _dq_flags, event_id, event_date, rma_id,
            event_subtype, order_id, customer_id, carrier_code, status
        ) VALUES (
            %(raw_event_id)s, %(dq_flags)s, %(event_id)s, %(event_date)s, %(rma_id)s,
            %(event_subtype)s, %(order_id)s, %(customer_id)s, %(carrier_code)s, %(status)s
        )
        ON CONFLICT (event_id) DO UPDATE SET
            raw_event_id  = EXCLUDED.raw_event_id,
            _dq_flags     = EXCLUDED._dq_flags,
            event_date    = EXCLUDED.event_date,
            rma_id        = EXCLUDED.rma_id,
            event_subtype = EXCLUDED.event_subtype,
            order_id      = EXCLUDED.order_id,
            customer_id   = EXCLUDED.customer_id,
            carrier_code  = EXCLUDED.carrier_code,
            status        = EXCLUDED.status
    """

    upsert_line_sql = """
        INSERT INTO staging.return_lines (
            raw_event_id, _dq_flags, rma_id, line_id, sku,
            quantity_returned, quantity_accepted, return_reason, resolution
        ) VALUES (
            %(raw_event_id)s, %(dq_flags)s, %(rma_id)s, %(line_id)s, %(sku)s,
            %(quantity_returned)s, %(quantity_accepted)s, %(return_reason)s, %(resolution)s
        )
        ON CONFLICT (rma_id, line_id) DO UPDATE SET
            raw_event_id      = EXCLUDED.raw_event_id,
            _dq_flags         = EXCLUDED._dq_flags,
            sku               = EXCLUDED.sku,
            quantity_returned = EXCLUDED.quantity_returned,
            quantity_accepted = EXCLUDED.quantity_accepted,
            return_reason     = EXCLUDED.return_reason,
            resolution        = EXCLUDED.resolution
    """

    header_count = 0
    line_count = 0
    for row in rows:
        p = row["payload"]

        if not p.get("rma_id") or not p.get("event_id"):
            continue

        conn.execute(upsert_header_sql, {
            "raw_event_id": row["id"],
            "dq_flags": None,
            "event_id": p.get("event_id"),
            "event_date": row["event_date"],
            "rma_id": p.get("rma_id"),
            "event_subtype": p.get("event_subtype"),
            "order_id": p.get("order_id"),
            "customer_id": p.get("customer_id"),
            "carrier_code": p.get("carrier_code"),
            "status": p.get("rma_status"),
        })
        header_count += 1

        for line in (p.get("lines") or []):
            conn.execute(upsert_line_sql, {
                "raw_event_id": row["id"],
                "dq_flags": None,
                "rma_id": p.get("rma_id"),
                "line_id": line.get("line_id"),
                "sku": line.get("sku"),
                "quantity_returned": line.get("quantity_returned"),
                "quantity_accepted": line.get("quantity_accepted"),
                "return_reason": line.get("return_reason"),
                "resolution": line.get("resolution"),
            })
            line_count += 1

    conn.commit()
    print(f"staging.returns: processed {header_count} raw events, {line_count} lines")
    total_h = conn.execute("SELECT COUNT(*) AS n FROM staging.returns").fetchone()["n"]
    total_l = conn.execute("SELECT COUNT(*) AS n FROM staging.return_lines").fetchone()["n"]
    print(f"staging.returns: {total_h} rows in table")
    print(f"staging.return_lines: {total_l} rows in table")


if __name__ == "__main__":
    with connect() as conn:
        load_loads(conn)
        load_carrier_events(conn)
        load_returns(conn)
