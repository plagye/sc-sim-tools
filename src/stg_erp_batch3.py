from db import connect


def load_supply_disruptions(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging.supply_disruptions (
            stg_id               BIGSERIAL PRIMARY KEY,
            raw_event_id         BIGINT REFERENCES raw.events(id),
            ingested_at          TIMESTAMPTZ DEFAULT now(),
            _dq_flags            TEXT[],
            event_id             TEXT NOT NULL,
            disruption_date      DATE,
            disruption_category  TEXT,
            disruption_subtype   TEXT,
            severity             TEXT,
            duration_days        INTEGER,
            start_date           DATE,
            end_date             DATE,
            affected_scope       TEXT,
            message              TEXT,
            CONSTRAINT supply_disruptions_event_id_uq UNIQUE (event_id)
        )
    """)
    conn.commit()

    rows = conn.execute("""
        SELECT id, event_date, payload
        FROM raw.events
        WHERE event_type = 'supply_disruptions'
          AND id > (SELECT COALESCE(MAX(raw_event_id), 0) FROM staging.supply_disruptions)
        ORDER BY id
    """).fetchall()

    inserted = 0
    for r in rows:
        p = r['payload']
        conn.execute("""
            INSERT INTO staging.supply_disruptions
                (raw_event_id, _dq_flags, event_id, disruption_date, disruption_category,
                 disruption_subtype, severity, duration_days, start_date, end_date,
                 affected_scope, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id        = EXCLUDED.raw_event_id,
                _dq_flags           = EXCLUDED._dq_flags,
                disruption_date     = EXCLUDED.disruption_date,
                disruption_category = EXCLUDED.disruption_category,
                disruption_subtype  = EXCLUDED.disruption_subtype,
                severity            = EXCLUDED.severity,
                duration_days       = EXCLUDED.duration_days,
                start_date          = EXCLUDED.start_date,
                end_date            = EXCLUDED.end_date,
                affected_scope      = EXCLUDED.affected_scope,
                message             = EXCLUDED.message,
                ingested_at         = now()
        """, (
            r['id'],
            None,
            p.get('event_id'),
            r['event_date'],
            p.get('disruption_category'),
            p.get('disruption_subtype'),
            p.get('severity'),
            p.get('duration_days'),
            p.get('start_date'),
            p.get('end_date'),
            p.get('affected_scope'),
            p.get('message'),
        ))
        inserted += 1

    conn.commit()
    return inserted


if __name__ == "__main__":
    with connect() as conn:
        n = load_supply_disruptions(conn)
        print(f"staging.supply_disruptions: {n} rows inserted/updated")

        row = conn.execute("SELECT COUNT(*) AS n FROM staging.supply_disruptions").fetchone()
        print(f"staging.supply_disruptions total: {row['n']} rows")
