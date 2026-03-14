import os
from datetime import date, timedelta
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


def _easter(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _polish_holidays(year):
    easter = _easter(year)
    fixed = {
        date(year, 1, 1),
        date(year, 1, 6),
        date(year, 5, 1),
        date(year, 5, 3),
        date(year, 8, 15),
        date(year, 11, 1),
        date(year, 11, 11),
        date(year, 12, 25),
        date(year, 12, 26),
    }
    moveable = {
        easter,
        easter + timedelta(days=1),
        easter + timedelta(days=60),
    }
    return fixed | moveable


def load_dim_date(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_date (
            date_id                  DATE PRIMARY KEY,
            day_of_week              INT,
            day_name                 TEXT,
            is_weekend               BOOLEAN,
            is_business_day          BOOLEAN,
            week_iso                 INT,
            month                    INT,
            month_name               TEXT,
            quarter                  INT,
            year                     INT,
            is_polish_public_holiday BOOLEAN
        )
    """)

    holidays = _polish_holidays(2026) | _polish_holidays(2027)

    start = date(2026, 1, 1)
    end = date(2027, 12, 31)
    rows = []
    d = start
    while d <= end:
        is_weekend = d.isoweekday() >= 6
        is_holiday = d in holidays
        rows.append((
            d,
            d.isoweekday(),
            d.strftime("%A"),
            is_weekend,
            not is_weekend and not is_holiday,
            d.isocalendar()[1],
            d.month,
            d.strftime("%B"),
            (d.month - 1) // 3 + 1,
            d.year,
            is_holiday,
        ))
        d += timedelta(days=1)

    cur.executemany("""
        INSERT INTO mart.dim_date (
            date_id, day_of_week, day_name, is_weekend, is_business_day,
            week_iso, month, month_name, quarter, year, is_polish_public_holiday
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_id) DO UPDATE SET
            day_of_week              = EXCLUDED.day_of_week,
            day_name                 = EXCLUDED.day_name,
            is_weekend               = EXCLUDED.is_weekend,
            is_business_day          = EXCLUDED.is_business_day,
            week_iso                 = EXCLUDED.week_iso,
            month                    = EXCLUDED.month,
            month_name               = EXCLUDED.month_name,
            quarter                  = EXCLUDED.quarter,
            year                     = EXCLUDED.year,
            is_polish_public_holiday = EXCLUDED.is_polish_public_holiday
    """, rows)
    print(f"dim_date: {len(rows)} rows upserted")


def load_dim_customer(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_customer (
            dim_customer_id       BIGSERIAL PRIMARY KEY,
            customer_id           TEXT NOT NULL,
            company_name          TEXT,
            segment               TEXT,
            country_code          TEXT,
            region                TEXT,
            credit_limit          NUMERIC(15,4),
            payment_terms_days    INT,
            currency              TEXT,
            preferred_carrier     TEXT,
            contract_discount_pct NUMERIC(6,4),
            active                BOOLEAN,
            onboarding_date       DATE,
            effective_from        DATE NOT NULL,
            effective_to          DATE,
            is_current            BOOLEAN NOT NULL,
            UNIQUE (customer_id, effective_from)
        )
    """)

    # Seed: one row per customer from dim.customers
    cur.execute("""
        INSERT INTO mart.dim_customer (
            customer_id, company_name, segment, country_code, region,
            credit_limit, payment_terms_days, currency, preferred_carrier,
            contract_discount_pct, active, onboarding_date,
            effective_from, effective_to, is_current
        )
        SELECT
            customer_id, company_name, segment, country_code, region,
            credit_limit::NUMERIC(15,4), payment_terms_days, currency, preferred_carrier,
            contract_discount_pct::NUMERIC(6,4), active, onboarding_date::DATE,
            onboarding_date::DATE, NULL, TRUE
        FROM dim.customers
        ON CONFLICT (customer_id, effective_from) DO NOTHING
    """)

    # SCD2 changes: segment and payment_terms_days only, ordered by change_date ASC
    cur.execute("""
        SELECT change_id, change_date, change_type, entity_id,
               old_value, new_value
        FROM staging.master_data_changes
        WHERE entity_type = 'customer'
          AND change_type IN ('segment', 'payment_terms_days')
        ORDER BY entity_id, change_date ASC
    """)
    changes = cur.fetchall()

    for ch in changes:
        customer_id = ch["entity_id"]
        change_date = ch["change_date"]
        change_type = ch["change_type"]
        new_value = ch["new_value"]

        # Find current active row for this customer
        cur.execute("""
            SELECT dim_customer_id, company_name, segment, country_code, region,
                   credit_limit, payment_terms_days, currency, preferred_carrier,
                   contract_discount_pct, active, onboarding_date, effective_from
            FROM mart.dim_customer
            WHERE customer_id = %s AND is_current = TRUE
        """, (customer_id,))
        current = cur.fetchone()
        if current is None:
            continue

        # Close the current row
        cur.execute("""
            UPDATE mart.dim_customer
            SET effective_to = %s, is_current = FALSE
            WHERE dim_customer_id = %s
        """, (change_date - timedelta(days=1), current["dim_customer_id"]))

        # Build new row attributes
        new_segment = new_value.get("segment", current["segment"]) if change_type == "segment" else current["segment"]
        new_payment_terms = new_value.get("payment_terms_days", current["payment_terms_days"]) if change_type == "payment_terms_days" else current["payment_terms_days"]

        cur.execute("""
            INSERT INTO mart.dim_customer (
                customer_id, company_name, segment, country_code, region,
                credit_limit, payment_terms_days, currency, preferred_carrier,
                contract_discount_pct, active, onboarding_date,
                effective_from, effective_to, is_current
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,TRUE)
            ON CONFLICT (customer_id, effective_from) DO NOTHING
        """, (
            customer_id,
            current["company_name"],
            new_segment,
            current["country_code"],
            current["region"],
            current["credit_limit"],
            new_payment_terms,
            current["currency"],
            current["preferred_carrier"],
            current["contract_discount_pct"],
            current["active"],
            current["onboarding_date"],
            change_date,
        ))

    cur.execute("SELECT COUNT(*) AS n FROM mart.dim_customer")
    total = cur.fetchone()["n"]
    cur.execute("SELECT COUNT(*) AS n FROM mart.dim_customer WHERE is_current")
    current_count = cur.fetchone()["n"]
    print(f"dim_customer: {total} total rows, {current_count} current")


def load_dim_sku(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_sku (
            dim_sku_id     BIGSERIAL PRIMARY KEY,
            sku            TEXT NOT NULL,
            valve_type     TEXT,
            dn             INT,
            material       TEXT,
            pressure_class TEXT,
            connection     TEXT,
            actuation      TEXT,
            weight_kg      NUMERIC(10,3),
            base_price_pln NUMERIC(15,4),
            base_price_eur NUMERIC(15,4),
            is_active      BOOLEAN,
            effective_from DATE NOT NULL,
            effective_to   DATE,
            is_current     BOOLEAN NOT NULL,
            UNIQUE (sku, effective_from)
        )
    """)

    # Seed from dim.catalog
    cur.execute("""
        INSERT INTO mart.dim_sku (
            sku, valve_type, dn, material, pressure_class, connection,
            actuation, weight_kg, base_price_pln, base_price_eur, is_active,
            effective_from, effective_to, is_current
        )
        SELECT
            sku, valve_type, dn, material, pressure_class, connection,
            actuation,
            weight_kg::NUMERIC(10,3),
            base_price_pln::NUMERIC(15,4),
            base_price_eur::NUMERIC(15,4),
            is_active,
            '2026-01-01'::DATE, NULL, TRUE
        FROM dim.catalog
        ON CONFLICT (sku, effective_from) DO NOTHING
    """)

    # SCD2: sku_status discontinuations
    cur.execute("""
        SELECT change_id, change_date, entity_id, new_value
        FROM staging.master_data_changes
        WHERE entity_type = 'product'
          AND change_type = 'sku_status'
        ORDER BY entity_id, change_date ASC
    """)
    changes = cur.fetchall()

    for ch in changes:
        sku = ch["entity_id"]
        change_date = ch["change_date"]

        cur.execute("""
            SELECT dim_sku_id, valve_type, dn, material, pressure_class, connection,
                   actuation, weight_kg, base_price_pln, base_price_eur, effective_from
            FROM mart.dim_sku
            WHERE sku = %s AND is_current = TRUE
        """, (sku,))
        current = cur.fetchone()
        if current is None:
            continue

        cur.execute("""
            UPDATE mart.dim_sku
            SET effective_to = %s, is_current = FALSE
            WHERE dim_sku_id = %s
        """, (change_date - timedelta(days=1), current["dim_sku_id"]))

        cur.execute("""
            INSERT INTO mart.dim_sku (
                sku, valve_type, dn, material, pressure_class, connection,
                actuation, weight_kg, base_price_pln, base_price_eur, is_active,
                effective_from, effective_to, is_current
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,%s,NULL,TRUE)
            ON CONFLICT (sku, effective_from) DO NOTHING
        """, (
            sku,
            current["valve_type"],
            current["dn"],
            current["material"],
            current["pressure_class"],
            current["connection"],
            current["actuation"],
            current["weight_kg"],
            current["base_price_pln"],
            current["base_price_eur"],
            change_date,
        ))

    cur.execute("SELECT COUNT(*) AS n FROM mart.dim_sku WHERE is_current AND NOT is_active")
    discontinued = cur.fetchone()["n"]
    print(f"dim_sku: seeded from catalog, {discontinued} current discontinued SKUs")


def load_dim_carrier(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_carrier (
            dim_carrier_id   BIGSERIAL PRIMARY KEY,
            carrier_code     TEXT NOT NULL UNIQUE,
            name             TEXT,
            base_reliability NUMERIC(4,3),
            transit_days_min INT,
            transit_days_max INT,
            cost_tier        TEXT,
            primary_use      TEXT,
            weight_unit      TEXT,
            is_current       BOOLEAN DEFAULT TRUE
        )
    """)

    cur.execute("""
        INSERT INTO mart.dim_carrier (
            carrier_code, name, base_reliability, transit_days_min, transit_days_max,
            cost_tier, primary_use, weight_unit, is_current
        )
        SELECT
            code, name, base_reliability::NUMERIC(4,3),
            transit_days_min, transit_days_max, cost_tier, primary_use, weight_unit, TRUE
        FROM dim.carriers
        ON CONFLICT (carrier_code) DO UPDATE SET
            name             = EXCLUDED.name,
            base_reliability = EXCLUDED.base_reliability,
            transit_days_min = EXCLUDED.transit_days_min,
            transit_days_max = EXCLUDED.transit_days_max,
            cost_tier        = EXCLUDED.cost_tier,
            primary_use      = EXCLUDED.primary_use,
            weight_unit      = EXCLUDED.weight_unit,
            is_current       = EXCLUDED.is_current
    """)

    # Apply the 1 carrier_base_rate change in-place
    cur.execute("""
        SELECT entity_id, new_value
        FROM staging.master_data_changes
        WHERE entity_type = 'carrier'
          AND change_type = 'carrier_base_rate'
    """)
    changes = cur.fetchall()
    for ch in changes:
        new_val = ch["new_value"]
        new_reliability = new_val.get("base_reliability")
        if new_reliability is not None:
            cur.execute("""
                UPDATE mart.dim_carrier
                SET base_reliability = %s
                WHERE carrier_code = %s
            """, (new_reliability, ch["entity_id"]))

    cur.execute("SELECT COUNT(*) AS n FROM mart.dim_carrier")
    print(f"dim_carrier: {cur.fetchone()['n']} rows")


def load_dim_warehouse(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.dim_warehouse (
            dim_warehouse_id BIGSERIAL PRIMARY KEY,
            warehouse_code   TEXT NOT NULL UNIQUE,
            name             TEXT,
            city             TEXT,
            country_code     TEXT,
            role             TEXT,
            inventory_share  NUMERIC(4,3)
        )
    """)

    cur.execute("""
        INSERT INTO mart.dim_warehouse (
            warehouse_code, name, city, country_code, role, inventory_share
        )
        SELECT
            code, name, city, country_code, role, inventory_share::NUMERIC(4,3)
        FROM dim.warehouses
        ON CONFLICT (warehouse_code) DO UPDATE SET
            name            = EXCLUDED.name,
            city            = EXCLUDED.city,
            country_code    = EXCLUDED.country_code,
            role            = EXCLUDED.role,
            inventory_share = EXCLUDED.inventory_share
    """)

    cur.execute("SELECT COUNT(*) AS n FROM mart.dim_warehouse")
    print(f"dim_warehouse: {cur.fetchone()['n']} rows")


def run():
    with connect() as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                load_dim_date(cur)
            with conn.transaction():
                load_dim_customer(cur)
            with conn.transaction():
                load_dim_sku(cur)
            with conn.transaction():
                load_dim_carrier(cur)
            with conn.transaction():
                load_dim_warehouse(cur)

        print("\n--- Verification ---")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM mart.dim_date")
            print(f"dim_date row count: {cur.fetchone()['n']}")

            cur.execute("""
                SELECT date_id, is_polish_public_holiday, is_business_day
                FROM mart.dim_date
                WHERE date_id IN ('2026-01-01','2026-04-06','2026-05-01','2026-12-25')
                ORDER BY date_id
            """)
            for r in cur.fetchall():
                print(f"  {r['date_id']}  holiday={r['is_polish_public_holiday']}  business={r['is_business_day']}")

            cur.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current FROM mart.dim_customer")
            r = cur.fetchone()
            print(f"dim_customer: total={r['total']}, current={r['current']}")

            cur.execute("SELECT COUNT(*) AS n FROM mart.dim_sku WHERE is_current AND NOT is_active")
            print(f"dim_sku discontinued (current): {cur.fetchone()['n']}")

            cur.execute("SELECT carrier_code, base_reliability FROM mart.dim_carrier ORDER BY carrier_code")
            for r in cur.fetchall():
                print(f"  carrier {r['carrier_code']}: reliability={r['base_reliability']}")

            cur.execute("SELECT warehouse_code, name FROM mart.dim_warehouse ORDER BY warehouse_code")
            for r in cur.fetchall():
                print(f"  warehouse {r['warehouse_code']}: {r['name']}")


if __name__ == "__main__":
    run()
