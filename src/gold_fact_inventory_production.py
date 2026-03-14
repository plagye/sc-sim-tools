from db import connect


def load_fact_inventory_daily(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_inventory_daily (
            fact_id          BIGSERIAL PRIMARY KEY,
            raw_event_id     BIGINT REFERENCES raw.events(id),
            ingested_at      TIMESTAMPTZ DEFAULT now(),
            dim_date_id      DATE REFERENCES mart.dim_date(date_id),
            dim_sku_id       BIGINT REFERENCES mart.dim_sku(dim_sku_id),
            dim_warehouse_id BIGINT REFERENCES mart.dim_warehouse(dim_warehouse_id),
            snapshot_date    DATE NOT NULL,
            sku              TEXT NOT NULL,
            warehouse_id     TEXT NOT NULL,
            qty_on_hand      INTEGER,
            qty_allocated    INTEGER,
            qty_available    INTEGER,
            safety_stock     INTEGER,
            reorder_point    INTEGER,
            target_stock     INTEGER,
            stockout_flag    BOOLEAN GENERATED ALWAYS AS (qty_available = 0) STORED,
            below_safety_stock BOOLEAN GENERATED ALWAYS AS (qty_available IS NOT NULL AND safety_stock IS NOT NULL AND qty_available < safety_stock) STORED,
            UNIQUE (snapshot_date, sku, warehouse_id)
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_inventory_daily (
            raw_event_id, dim_date_id, dim_sku_id, dim_warehouse_id,
            snapshot_date, sku, warehouse_id,
            qty_on_hand, qty_allocated, qty_available,
            safety_stock, reorder_point, target_stock
        )
        SELECT
            s.raw_event_id,
            CASE WHEN s.snapshot_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN s.snapshot_date ELSE NULL END,
            ds.dim_sku_id,
            dw.dim_warehouse_id,
            s.snapshot_date, s.sku, s.warehouse_id,
            s.qty_on_hand, s.qty_allocated, s.qty_available,
            it.safety_stock, it.reorder_point, it.target_stock
        FROM staging.inventory_snapshots s
        JOIN LATERAL (
            SELECT dim_sku_id FROM mart.dim_sku
            WHERE sku = s.sku
              AND effective_from <= s.snapshot_date
              AND (effective_to IS NULL OR effective_to >= s.snapshot_date)
            ORDER BY effective_from DESC LIMIT 1
        ) ds ON TRUE
        JOIN LATERAL (
            SELECT dim_warehouse_id FROM mart.dim_warehouse
            WHERE warehouse_code = s.warehouse_id LIMIT 1
        ) dw ON TRUE
        LEFT JOIN staging.inventory_targets it
            ON it.sku = s.sku
            AND it.warehouse_id = s.warehouse_id
            AND it.target_month = date_trunc('month', s.snapshot_date)::date
        ON CONFLICT (snapshot_date, sku, warehouse_id) DO UPDATE SET
            qty_on_hand      = EXCLUDED.qty_on_hand,
            qty_allocated    = EXCLUDED.qty_allocated,
            qty_available    = EXCLUDED.qty_available,
            safety_stock     = EXCLUDED.safety_stock,
            reorder_point    = EXCLUDED.reorder_point,
            target_stock     = EXCLUDED.target_stock,
            ingested_at      = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_inventory_daily")
    print(f"fact_inventory_daily: {cur.fetchone()['n']} rows")


def load_fact_production(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.fact_production (
            fact_id          BIGSERIAL PRIMARY KEY,
            raw_event_id     BIGINT REFERENCES raw.events(id),
            ingested_at      TIMESTAMPTZ DEFAULT now(),
            dim_date_id      DATE REFERENCES mart.dim_date(date_id),
            dim_sku_id       BIGINT REFERENCES mart.dim_sku(dim_sku_id),
            dim_warehouse_id BIGINT REFERENCES mart.dim_warehouse(dim_warehouse_id),
            event_id         TEXT NOT NULL UNIQUE,
            completion_date  DATE,
            batch_id         TEXT,
            sku              TEXT,
            qty_completed    INTEGER,
            grade            TEXT,
            production_line  TEXT,
            warehouse_id     TEXT
        )
    """)

    cur.execute("""
        INSERT INTO mart.fact_production (
            raw_event_id, dim_date_id, dim_sku_id, dim_warehouse_id,
            event_id, completion_date, batch_id, sku,
            qty_completed, grade, production_line, warehouse_id
        )
        SELECT
            p.raw_event_id,
            CASE WHEN p.completion_date BETWEEN '2026-01-01' AND '2027-12-31'
                 THEN p.completion_date ELSE NULL END,
            ds.dim_sku_id,
            dw.dim_warehouse_id,
            p.event_id, p.completion_date, p.batch_id, p.sku,
            p.quantity, p.grade, p.production_line, p.warehouse_id
        FROM staging.production_completions p
        JOIN LATERAL (
            SELECT dim_sku_id FROM mart.dim_sku
            WHERE sku = p.sku
              AND effective_from <= p.completion_date
              AND (effective_to IS NULL OR effective_to >= p.completion_date)
            ORDER BY effective_from DESC LIMIT 1
        ) ds ON TRUE
        JOIN LATERAL (
            SELECT dim_warehouse_id FROM mart.dim_warehouse
            WHERE warehouse_code = p.warehouse_id LIMIT 1
        ) dw ON TRUE
        ON CONFLICT (event_id) DO UPDATE SET
            qty_completed    = EXCLUDED.qty_completed,
            grade            = EXCLUDED.grade,
            ingested_at      = now()
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.fact_production")
    print(f"fact_production: {cur.fetchone()['n']} rows")


def run():
    with connect() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                load_fact_inventory_daily(cur)
            with conn.transaction():
                load_fact_production(cur)


if __name__ == "__main__":
    run()
