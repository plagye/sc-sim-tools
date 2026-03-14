from db import connect


def process_exchange_rates(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.exchange_rates (
            stg_id        BIGSERIAL PRIMARY KEY,
            raw_event_id  BIGINT REFERENCES raw.events(id),
            ingested_at   TIMESTAMPTZ DEFAULT now(),
            _dq_flags     TEXT[],
            rate_date     DATE NOT NULL,
            currency_pair TEXT NOT NULL,
            rate          NUMERIC(15,6),
            change_pct    NUMERIC(8,4),
            UNIQUE (rate_date, currency_pair)
        )
    """)
    conn.execute("ALTER TABLE staging.exchange_rates ALTER COLUMN rate DROP NOT NULL")
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.exchange_rates"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, event_date, payload FROM raw.events "
        "WHERE event_type = 'exchange_rates' AND id > %s",
        (watermark,),
    ).fetchall()

    upserted = 0
    for row in rows:
        p = row["payload"]
        rate_date = p.get("simulation_date") or str(row["event_date"])
        currency_pair = p.get("currency_pair")
        rate = p.get("rate")
        change_pct = p.get("change_pct")
        flags = []
        if currency_pair is None:
            flags.append("missing_currency_pair")
            currency_pair = "UNKNOWN"
        if rate is None:
            flags.append("missing_rate")

        conn.execute("""
            INSERT INTO staging.exchange_rates
                (raw_event_id, rate_date, currency_pair, rate, change_pct, _dq_flags)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (rate_date, currency_pair) DO UPDATE SET
                raw_event_id  = EXCLUDED.raw_event_id,
                rate          = EXCLUDED.rate,
                change_pct    = EXCLUDED.change_pct,
                ingested_at   = now(),
                _dq_flags     = EXCLUDED._dq_flags
        """, (row["id"], rate_date, currency_pair, rate, change_pct, flags))
        upserted += 1

    conn.commit()
    return upserted


def process_inventory_snapshots(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.inventory_snapshots (
            stg_id        BIGSERIAL PRIMARY KEY,
            raw_event_id  BIGINT REFERENCES raw.events(id),
            ingested_at   TIMESTAMPTZ DEFAULT now(),
            _dq_flags     TEXT[],
            snapshot_date DATE NOT NULL,
            warehouse_id  TEXT NOT NULL,
            sku           TEXT NOT NULL,
            qty_on_hand   INTEGER,
            qty_allocated INTEGER,
            qty_available INTEGER,
            snapshot_id   TEXT,
            UNIQUE (snapshot_date, warehouse_id, sku)
        )
    """)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.inventory_snapshots"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, event_date, payload FROM raw.events "
        "WHERE event_type = 'inventory_snapshots' AND id > %s",
        (watermark,),
    ).fetchall()

    params = []
    for row in rows:
        p = row["payload"]
        snapshot_date = str(row["event_date"])
        warehouse_id = p.get("warehouse")
        snapshot_id = p.get("snapshot_id")
        flags = []
        if warehouse_id is None and snapshot_id:
            # e.g. SNAP-20260827-W02 → last segment is the warehouse
            warehouse_id = snapshot_id.rsplit("-", 1)[-1]
            flags.append("warehouse_derived_from_snapshot_id")
        for pos in p.get("positions", []):
            params.append((
                row["id"], snapshot_date, warehouse_id, pos["sku"],
                pos.get("quantity_on_hand"), pos.get("quantity_allocated"),
                pos.get("quantity_available"), snapshot_id, flags,
            ))

    if params:
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_inv_snap (
                raw_event_id  BIGINT,
                snapshot_date DATE,
                warehouse_id  TEXT,
                sku           TEXT,
                qty_on_hand   INTEGER,
                qty_allocated INTEGER,
                qty_available INTEGER,
                snapshot_id   TEXT,
                _dq_flags     TEXT[]
            ) ON COMMIT DROP
        """)
        with conn.cursor().copy(
            "COPY tmp_inv_snap (raw_event_id, snapshot_date, warehouse_id, sku, "
            "qty_on_hand, qty_allocated, qty_available, snapshot_id, _dq_flags) FROM STDIN"
        ) as copy:
            for p in params:
                # format TEXT[] as postgres literal: {flag1,flag2} or {}
                flags_str = "{" + ",".join(p[8]) + "}"
                copy.write_row(p[:8] + (flags_str,))
        conn.execute("""
            INSERT INTO staging.inventory_snapshots
                (raw_event_id, snapshot_date, warehouse_id, sku,
                 qty_on_hand, qty_allocated, qty_available, snapshot_id, _dq_flags)
            SELECT raw_event_id, snapshot_date, warehouse_id, sku,
                   qty_on_hand, qty_allocated, qty_available, snapshot_id, _dq_flags
            FROM tmp_inv_snap
            ON CONFLICT (snapshot_date, warehouse_id, sku) DO UPDATE SET
                raw_event_id  = EXCLUDED.raw_event_id,
                qty_on_hand   = EXCLUDED.qty_on_hand,
                qty_allocated = EXCLUDED.qty_allocated,
                qty_available = EXCLUDED.qty_available,
                snapshot_id   = EXCLUDED.snapshot_id,
                ingested_at   = now(),
                _dq_flags     = EXCLUDED._dq_flags
        """)

    conn.commit()
    return len(params)


def process_inventory_movements(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.inventory_movements (
            stg_id         BIGSERIAL PRIMARY KEY,
            raw_event_id   BIGINT REFERENCES raw.events(id),
            ingested_at    TIMESTAMPTZ DEFAULT now(),
            _dq_flags      TEXT[],
            event_id       TEXT NOT NULL UNIQUE,
            movement_date  DATE,
            movement_type  TEXT,
            direction      TEXT,
            warehouse_id   TEXT,
            sku            TEXT,
            quantity       INTEGER,
            order_reference TEXT,
            reason_code    TEXT
        )
    """)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.inventory_movements"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, payload FROM raw.events "
        "WHERE event_type = 'inventory_movements' AND id > %s",
        (watermark,),
    ).fetchall()

    params = []
    for row in rows:
        p = row["payload"]
        event_id = p.get("event_id")
        flags = []
        if event_id is None:
            event_id = p.get("movement_id")
            flags.append("event_id_missing_used_movement_id")
        params.append((
            row["id"],
            event_id,
            p.get("movement_date"),
            p.get("movement_type"),
            p.get("quantity_direction"),
            p.get("warehouse_id"),
            p.get("sku"),
            p.get("quantity"),
            p.get("order_id"),
            p.get("reason_code"),
            flags,
        ))
    if params:
        conn.cursor().executemany("""
            INSERT INTO staging.inventory_movements
                (raw_event_id, event_id, movement_date, movement_type, direction,
                 warehouse_id, sku, quantity, order_reference, reason_code, _dq_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id    = EXCLUDED.raw_event_id,
                movement_date   = EXCLUDED.movement_date,
                movement_type   = EXCLUDED.movement_type,
                direction       = EXCLUDED.direction,
                warehouse_id    = EXCLUDED.warehouse_id,
                sku             = EXCLUDED.sku,
                quantity        = EXCLUDED.quantity,
                order_reference = EXCLUDED.order_reference,
                reason_code     = EXCLUDED.reason_code,
                ingested_at     = now(),
                _dq_flags       = EXCLUDED._dq_flags
        """, params)
    conn.commit()
    return len(params)


def process_credit_events(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.credit_events (
            stg_id            BIGSERIAL PRIMARY KEY,
            raw_event_id      BIGINT REFERENCES raw.events(id),
            ingested_at       TIMESTAMPTZ DEFAULT now(),
            _dq_flags         TEXT[],
            event_id          TEXT NOT NULL UNIQUE,
            event_date        DATE,
            customer_id       TEXT,
            event_subtype     TEXT,
            customer_balance  NUMERIC(15,4),
            open_order_value  NUMERIC(15,4),
            credit_limit      NUMERIC(15,4),
            exposure          NUMERIC(15,4)
        )
    """)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.credit_events"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, payload FROM raw.events "
        "WHERE event_type = 'credit_events' AND id > %s",
        (watermark,),
    ).fetchall()

    params = []
    for row in rows:
        p = row["payload"]
        event_id = p.get("event_id")
        flags = []
        if event_id is None:
            event_id = f"raw:{row['id']}"
            flags.append("event_id_missing_synthetic_key")
        params.append((
            row["id"],
            event_id,
            p.get("simulation_date"),
            p.get("customer_id"),
            p.get("event_type"),
            p.get("customer_balance_pln"),
            p.get("open_orders_value_pln"),
            p.get("credit_limit_pln"),
            p.get("exposure_pln"),
            flags,
        ))
    if params:
        conn.cursor().executemany("""
            INSERT INTO staging.credit_events
                (raw_event_id, event_id, event_date, customer_id, event_subtype,
                 customer_balance, open_order_value, credit_limit, exposure, _dq_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id     = EXCLUDED.raw_event_id,
                event_date       = EXCLUDED.event_date,
                customer_id      = EXCLUDED.customer_id,
                event_subtype    = EXCLUDED.event_subtype,
                customer_balance = EXCLUDED.customer_balance,
                open_order_value = EXCLUDED.open_order_value,
                credit_limit     = EXCLUDED.credit_limit,
                exposure         = EXCLUDED.exposure,
                ingested_at      = now(),
                _dq_flags        = EXCLUDED._dq_flags
        """, params)
    conn.commit()
    return len(params)


def process_production_completions(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.production_completions (
            stg_id           BIGSERIAL PRIMARY KEY,
            raw_event_id     BIGINT REFERENCES raw.events(id),
            ingested_at      TIMESTAMPTZ DEFAULT now(),
            _dq_flags        TEXT[],
            event_id         TEXT NOT NULL UNIQUE,
            completion_date  DATE,
            batch_id         TEXT,
            sku              TEXT,
            quantity         INTEGER,
            grade            TEXT,
            production_line  TEXT,
            warehouse_id     TEXT
        )
    """)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.production_completions"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, payload FROM raw.events "
        "WHERE event_type = 'production_completions' AND id > %s",
        (watermark,),
    ).fetchall()

    params = []
    for row in rows:
        p = row["payload"]
        event_id = p.get("event_id")
        grade = p.get("grade")
        flags = []
        if event_id is None:
            event_id = f"raw:{row['id']}"
            flags.append("event_id_missing_synthetic_key")
        if grade not in ("A", "B", None):
            flags.append(f"unexpected_grade:{grade}")
        params.append((
            row["id"],
            event_id,
            p.get("simulation_date"),
            p.get("batch_id"),
            p.get("sku"),
            p.get("quantity"),
            grade,
            str(p["production_line"]) if p.get("production_line") is not None else None,
            p.get("warehouse"),
            flags,
        ))
    if params:
        conn.cursor().executemany("""
            INSERT INTO staging.production_completions
                (raw_event_id, event_id, completion_date, batch_id, sku,
                 quantity, grade, production_line, warehouse_id, _dq_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id    = EXCLUDED.raw_event_id,
                completion_date = EXCLUDED.completion_date,
                batch_id        = EXCLUDED.batch_id,
                sku             = EXCLUDED.sku,
                quantity        = EXCLUDED.quantity,
                grade           = EXCLUDED.grade,
                production_line = EXCLUDED.production_line,
                warehouse_id    = EXCLUDED.warehouse_id,
                ingested_at     = now(),
                _dq_flags       = EXCLUDED._dq_flags
        """, params)
    conn.commit()
    return len(params)


def process_backorder_events(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.backorder_events (
            stg_id              BIGSERIAL PRIMARY KEY,
            raw_event_id        BIGINT REFERENCES raw.events(id),
            ingested_at         TIMESTAMPTZ DEFAULT now(),
            _dq_flags           TEXT[],
            event_id            TEXT NOT NULL UNIQUE,
            event_date          DATE,
            order_id            TEXT,
            order_line_id       TEXT,
            sku                 TEXT,
            quantity            INTEGER,
            reason              TEXT,
            escalated_to_express BOOLEAN
        )
    """)
    conn.commit()

    watermark = conn.execute(
        "SELECT COALESCE(MAX(raw_event_id), 0) AS wm FROM staging.backorder_events"
    ).fetchone()["wm"]

    rows = conn.execute(
        "SELECT id, payload FROM raw.events "
        "WHERE event_type = 'backorder_events' AND id > %s",
        (watermark,),
    ).fetchall()

    params = []
    for row in rows:
        p = row["payload"]
        event_id = p.get("event_id")
        flags = []
        if event_id is None:
            event_id = f"raw:{row['id']}"
            flags.append("event_id_missing_synthetic_key")
        qty = p.get("quantity_backordered")
        if qty is not None and qty > 1_000_000:
            qty = None
            flags.append("qty_overflow")
        params.append((
            row["id"],
            event_id,
            p.get("backorder_date"),
            p.get("order_id"),
            p.get("order_line_id"),
            p.get("sku"),
            qty,
            p.get("backorder_reason"),
            p.get("escalated_to_express"),
            flags,
        ))
    if params:
        conn.cursor().executemany("""
            INSERT INTO staging.backorder_events
                (raw_event_id, event_id, event_date, order_id, order_line_id,
                 sku, quantity, reason, escalated_to_express, _dq_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id         = EXCLUDED.raw_event_id,
                event_date           = EXCLUDED.event_date,
                order_id             = EXCLUDED.order_id,
                order_line_id        = EXCLUDED.order_line_id,
                sku                  = EXCLUDED.sku,
                quantity             = EXCLUDED.quantity,
                reason               = EXCLUDED.reason,
                escalated_to_express = EXCLUDED.escalated_to_express,
                ingested_at          = now(),
                _dq_flags            = EXCLUDED._dq_flags
        """, params)
    conn.commit()
    return len(params)


if __name__ == "__main__":
    with connect() as conn:
        n = process_exchange_rates(conn)
        print(f"exchange_rates:         {n:>7} rows upserted")

        n = process_inventory_snapshots(conn)
        print(f"inventory_snapshots:    {n:>7} rows upserted")

        n = process_inventory_movements(conn)
        print(f"inventory_movements:    {n:>7} rows upserted")

        n = process_credit_events(conn)
        print(f"credit_events:          {n:>7} rows upserted")

        n = process_production_completions(conn)
        print(f"production_completions: {n:>7} rows upserted")

        n = process_backorder_events(conn)
        print(f"backorder_events:       {n:>7} rows upserted")
