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


def load_payments(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.payments (
            stg_id           BIGSERIAL PRIMARY KEY,
            raw_event_id     BIGINT REFERENCES raw.events(id),
            ingested_at      TIMESTAMPTZ DEFAULT now(),
            _dq_flags        TEXT[],
            event_id         TEXT,
            payment_date     DATE,
            customer_id      TEXT,
            amount           NUMERIC(15,4),
            payment_behaviour TEXT,
            balance_before   NUMERIC(15,4),
            balance_after    NUMERIC(15,4)
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS payments_event_id_ux ON staging.payments (event_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, payload
        FROM raw.events
        WHERE event_type = 'payments'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.payments)
    """).fetchall()

    VALID_BEHAVIOURS = {'on_time', 'late', 'very_late'}
    inserted = 0
    for r in rows:
        p = r['payload']
        flags = []
        behaviour = p.get('payment_behaviour')
        if behaviour not in VALID_BEHAVIOURS:
            flags.append('unexpected_payment_behaviour')
        conn.execute("""
            INSERT INTO staging.payments
                (raw_event_id, _dq_flags, event_id, payment_date, customer_id,
                 amount, payment_behaviour, balance_before, balance_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id     = EXCLUDED.raw_event_id,
                _dq_flags        = EXCLUDED._dq_flags,
                payment_date     = EXCLUDED.payment_date,
                customer_id      = EXCLUDED.customer_id,
                amount           = EXCLUDED.amount,
                payment_behaviour = EXCLUDED.payment_behaviour,
                balance_before   = EXCLUDED.balance_before,
                balance_after    = EXCLUDED.balance_after
        """, (
            r['id'],
            flags or None,
            p.get('event_id'),
            p.get('simulation_date'),
            p.get('customer_id'),
            p.get('payment_amount_pln'),
            behaviour,
            p.get('balance_before_pln'),
            p.get('balance_after_pln'),
        ))
        inserted += 1
    conn.commit()
    return inserted


def load_master_data_changes(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.master_data_changes (
            stg_id       BIGSERIAL PRIMARY KEY,
            raw_event_id BIGINT REFERENCES raw.events(id),
            ingested_at  TIMESTAMPTZ DEFAULT now(),
            _dq_flags    TEXT[],
            change_id    TEXT,
            change_date  DATE,
            change_type  TEXT,
            entity_type  TEXT,
            entity_id    TEXT,
            old_value    JSONB,
            new_value    JSONB
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS master_data_changes_change_id_ux ON staging.master_data_changes (change_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, payload
        FROM raw.events
        WHERE event_type = 'master_data_changes'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.master_data_changes)
    """).fetchall()

    import json
    inserted = 0
    for r in rows:
        p = r['payload']
        old_val = p.get('old_value')
        new_val = p.get('new_value')
        # Wrap scalar values in a JSON object so the column stays JSONB
        if not isinstance(old_val, dict):
            old_val = {'value': old_val}
        if not isinstance(new_val, dict):
            new_val = {'value': new_val}
        conn.execute("""
            INSERT INTO staging.master_data_changes
                (raw_event_id, _dq_flags, change_id, change_date, change_type,
                 entity_type, entity_id, old_value, new_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (change_id) DO UPDATE SET
                raw_event_id = EXCLUDED.raw_event_id,
                _dq_flags    = EXCLUDED._dq_flags,
                change_date  = EXCLUDED.change_date,
                change_type  = EXCLUDED.change_type,
                entity_type  = EXCLUDED.entity_type,
                entity_id    = EXCLUDED.entity_id,
                old_value    = EXCLUDED.old_value,
                new_value    = EXCLUDED.new_value
        """, (
            r['id'],
            None,
            p.get('change_id'),
            p.get('effective_date'),
            p.get('field_changed'),
            p.get('entity_type'),
            p.get('entity_id'),
            json.dumps(old_val),
            json.dumps(new_val),
        ))
        inserted += 1
    conn.commit()
    return inserted


def load_production_reclassifications(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.production_reclassifications (
            stg_id                  BIGSERIAL PRIMARY KEY,
            raw_event_id            BIGINT REFERENCES raw.events(id),
            ingested_at             TIMESTAMPTZ DEFAULT now(),
            _dq_flags               TEXT[],
            event_id                TEXT,
            reclassification_date   DATE,
            batch_id                TEXT,
            sku                     TEXT,
            quantity                INTEGER,
            original_grade          TEXT,
            new_grade               TEXT,
            reclassification_reason TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS production_reclassifications_event_id_ux ON staging.production_reclassifications (event_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, payload
        FROM raw.events
        WHERE event_type = 'production_reclassifications'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.production_reclassifications)
    """).fetchall()

    inserted = 0
    for r in rows:
        p = r['payload']
        conn.execute("""
            INSERT INTO staging.production_reclassifications
                (raw_event_id, _dq_flags, event_id, reclassification_date, batch_id,
                 sku, quantity, original_grade, new_grade, reclassification_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id            = EXCLUDED.raw_event_id,
                _dq_flags               = EXCLUDED._dq_flags,
                reclassification_date   = EXCLUDED.reclassification_date,
                batch_id                = EXCLUDED.batch_id,
                sku                     = EXCLUDED.sku,
                quantity                = EXCLUDED.quantity,
                original_grade          = EXCLUDED.original_grade,
                new_grade               = EXCLUDED.new_grade,
                reclassification_reason = EXCLUDED.reclassification_reason
        """, (
            r['id'],
            None,
            p.get('event_id'),
            p.get('simulation_date'),
            p.get('batch_id'),
            p.get('sku'),
            p.get('quantity'),
            p.get('original_grade'),
            p.get('new_grade'),
            p.get('reclassification_reason'),
        ))
        inserted += 1
    conn.commit()
    return inserted


def load_order_cancellations(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.order_cancellations (
            stg_id             BIGSERIAL PRIMARY KEY,
            raw_event_id       BIGINT REFERENCES raw.events(id),
            ingested_at        TIMESTAMPTZ DEFAULT now(),
            _dq_flags          TEXT[],
            event_id           TEXT,
            cancellation_date  DATE,
            order_id           TEXT,
            order_line_id      TEXT,
            sku                TEXT,
            quantity_cancelled INTEGER,
            reason             TEXT,
            days_backordered   INTEGER,
            cancellation_type  TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS order_cancellations_event_id_ux ON staging.order_cancellations (event_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, payload
        FROM raw.events
        WHERE event_type = 'order_cancellations'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.order_cancellations)
    """).fetchall()

    inserted = 0
    for r in rows:
        p = r['payload']
        flags = []
        raw_qty = p.get('quantity_cancelled')
        if raw_qty is not None and raw_qty > 1_000_000:
            flags.append('qty_overflow')
            raw_qty = None
        conn.execute("""
            INSERT INTO staging.order_cancellations
                (raw_event_id, _dq_flags, event_id, cancellation_date, order_id,
                 order_line_id, sku, quantity_cancelled, reason, days_backordered,
                 cancellation_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id       = EXCLUDED.raw_event_id,
                _dq_flags          = EXCLUDED._dq_flags,
                cancellation_date  = EXCLUDED.cancellation_date,
                order_id           = EXCLUDED.order_id,
                order_line_id      = EXCLUDED.order_line_id,
                sku                = EXCLUDED.sku,
                quantity_cancelled = EXCLUDED.quantity_cancelled,
                reason             = EXCLUDED.reason,
                days_backordered   = EXCLUDED.days_backordered,
                cancellation_type  = EXCLUDED.cancellation_type
        """, (
            r['id'],
            flags or None,
            p.get('event_id'),
            p.get('cancellation_date'),
            p.get('order_id'),
            p.get('order_line_id'),
            p.get('sku'),
            raw_qty,
            p.get('reason'),
            p.get('days_backordered'),
            p.get('cancellation_type'),
        ))
        inserted += 1
    conn.commit()
    return inserted


def load_order_modifications(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.order_modifications (
            stg_id            BIGSERIAL PRIMARY KEY,
            raw_event_id      BIGINT REFERENCES raw.events(id),
            ingested_at       TIMESTAMPTZ DEFAULT now(),
            _dq_flags         TEXT[],
            event_id          TEXT,
            modification_date DATE,
            order_id          TEXT,
            line_id           TEXT,
            modification_type TEXT,
            old_value         TEXT,
            new_value         TEXT,
            reason            TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS order_modifications_event_id_ux ON staging.order_modifications (event_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, payload
        FROM raw.events
        WHERE event_type = 'order_modifications'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.order_modifications)
    """).fetchall()

    inserted = 0
    for r in rows:
        p = r['payload']
        conn.execute("""
            INSERT INTO staging.order_modifications
                (raw_event_id, _dq_flags, event_id, modification_date, order_id,
                 line_id, modification_type, old_value, new_value, reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id      = EXCLUDED.raw_event_id,
                _dq_flags         = EXCLUDED._dq_flags,
                modification_date = EXCLUDED.modification_date,
                order_id          = EXCLUDED.order_id,
                line_id           = EXCLUDED.line_id,
                modification_type = EXCLUDED.modification_type,
                old_value         = EXCLUDED.old_value,
                new_value         = EXCLUDED.new_value,
                reason            = EXCLUDED.reason
        """, (
            r['id'],
            None,
            p.get('event_id'),
            p.get('modification_date'),
            p.get('order_id'),
            p.get('line_id'),
            p.get('modification_type'),
            str(p['old_value']) if p.get('old_value') is not None else None,
            str(p['new_value']) if p.get('new_value') is not None else None,
            p.get('reason'),
        ))
        inserted += 1
    conn.commit()
    return inserted


def load_schema_evolution_events(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.schema_evolution_events (
            stg_id          BIGSERIAL PRIMARY KEY,
            raw_event_id    BIGINT REFERENCES raw.events(id),
            ingested_at     TIMESTAMPTZ DEFAULT now(),
            _dq_flags       TEXT[],
            evolution_id    TEXT,
            event_date      DATE,
            field_name      TEXT,
            sim_day         INTEGER,
            activation_date DATE,
            description     TEXT
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS schema_evolution_events_evolution_id_ux ON staging.schema_evolution_events (evolution_id)")
    conn.commit()

    rows = conn.execute("""
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'schema_evolution_events'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.schema_evolution_events)
    """).fetchall()

    inserted = 0
    for r in rows:
        p = r['payload']
        # activation_date: parse from timestamp field (YYYY-MM-DDTHH:MM:SSZ)
        ts = p.get('timestamp', '')
        activation_date = ts[:10] if ts else None
        conn.execute("""
            INSERT INTO staging.schema_evolution_events
                (raw_event_id, _dq_flags, evolution_id, event_date, field_name,
                 sim_day, activation_date, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (evolution_id) DO UPDATE SET
                raw_event_id    = EXCLUDED.raw_event_id,
                _dq_flags       = EXCLUDED._dq_flags,
                event_date      = EXCLUDED.event_date,
                field_name      = EXCLUDED.field_name,
                sim_day         = EXCLUDED.sim_day,
                activation_date = EXCLUDED.activation_date,
                description     = EXCLUDED.description
        """, (
            r['id'],
            None,
            p.get('evolution_id'),
            r['event_date'],
            p.get('field_name'),
            p.get('effective_from_day'),
            activation_date,
            p.get('description'),
        ))
        inserted += 1
    conn.commit()
    return inserted


if __name__ == '__main__':
    with connect() as conn:
        n = load_payments(conn)
        print(f'staging.payments:                  {n} rows inserted/updated')

        n = load_master_data_changes(conn)
        print(f'staging.master_data_changes:       {n} rows inserted/updated')

        n = load_production_reclassifications(conn)
        print(f'staging.production_reclassifications: {n} rows inserted/updated')

        n = load_order_cancellations(conn)
        print(f'staging.order_cancellations:       {n} rows inserted/updated')

        n = load_order_modifications(conn)
        print(f'staging.order_modifications:       {n} rows inserted/updated')

        n = load_schema_evolution_events(conn)
        print(f'staging.schema_evolution_events:   {n} rows inserted/updated')

    # Final row counts
    with connect() as conn:
        print()
        for tbl in [
            'staging.payments',
            'staging.master_data_changes',
            'staging.production_reclassifications',
            'staging.order_cancellations',
            'staging.order_modifications',
            'staging.schema_evolution_events',
        ]:
            row = conn.execute(f'SELECT COUNT(*) AS n FROM {tbl}').fetchone()
            print(f'{tbl}: {row["n"]} total rows')
