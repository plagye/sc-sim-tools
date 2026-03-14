import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()
log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ["MASTER_DATA_DIR"])


def _connect():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"],
        sslmode=os.environ["DB_SSL"],
        row_factory=dict_row,
    )


def _load_carriers(cur):
    rows = json.loads((DATA_DIR / "carriers.json").read_text())
    cur.executemany(
        """
        INSERT INTO dim.carriers
            (code, name, base_reliability, transit_days_min, transit_days_max,
             cost_tier, primary_use, weight_unit)
        VALUES
            (%(code)s, %(name)s, %(base_reliability)s, %(transit_days_min)s,
             %(transit_days_max)s, %(cost_tier)s, %(primary_use)s, %(weight_unit)s)
        ON CONFLICT (code) DO UPDATE SET
            name             = EXCLUDED.name,
            base_reliability = EXCLUDED.base_reliability,
            transit_days_min = EXCLUDED.transit_days_min,
            transit_days_max = EXCLUDED.transit_days_max,
            cost_tier        = EXCLUDED.cost_tier,
            primary_use      = EXCLUDED.primary_use,
            weight_unit      = EXCLUDED.weight_unit
        """,
        rows,
    )
    log.info("carriers: %d rows upserted", len(rows))


def _load_catalog(cur):
    rows = json.loads((DATA_DIR / "catalog.json").read_text())
    cur.executemany(
        """
        INSERT INTO dim.catalog
            (sku, valve_type, dn, material, pressure_class, connection,
             actuation, weight_kg, base_price_pln, base_price_eur, is_active)
        VALUES
            (%(sku)s, %(valve_type)s, %(dn)s, %(material)s, %(pressure_class)s,
             %(connection)s, %(actuation)s, %(weight_kg)s, %(base_price_pln)s,
             %(base_price_eur)s, %(is_active)s)
        ON CONFLICT (sku) DO UPDATE SET
            valve_type     = EXCLUDED.valve_type,
            dn             = EXCLUDED.dn,
            material       = EXCLUDED.material,
            pressure_class = EXCLUDED.pressure_class,
            connection     = EXCLUDED.connection,
            actuation      = EXCLUDED.actuation,
            weight_kg      = EXCLUDED.weight_kg,
            base_price_pln = EXCLUDED.base_price_pln,
            base_price_eur = EXCLUDED.base_price_eur,
            is_active      = EXCLUDED.is_active
        """,
        rows,
    )
    log.info("catalog: %d rows upserted", len(rows))


def _load_customers(cur):
    rows = json.loads((DATA_DIR / "customers.json").read_text())
    for r in rows:
        for col in ("primary_address", "secondary_address", "ordering_profile",
                    "seasonal_profile", "shutdown_months"):
            if r.get(col) is not None:
                r[col] = json.dumps(r[col])
    cur.executemany(
        """
        INSERT INTO dim.customers
            (customer_id, company_name, segment, country_code, region,
             primary_address, secondary_address, credit_limit, payment_terms_days,
             currency, preferred_carrier, contract_discount_pct, ordering_profile,
             seasonal_profile, shutdown_months, accepts_deliveries_december,
             active, onboarding_date)
        VALUES
            (%(customer_id)s, %(company_name)s, %(segment)s, %(country_code)s,
             %(region)s, %(primary_address)s, %(secondary_address)s, %(credit_limit)s,
             %(payment_terms_days)s, %(currency)s, %(preferred_carrier)s,
             %(contract_discount_pct)s, %(ordering_profile)s, %(seasonal_profile)s,
             %(shutdown_months)s, %(accepts_deliveries_december)s, %(active)s,
             %(onboarding_date)s)
        ON CONFLICT (customer_id) DO UPDATE SET
            company_name               = EXCLUDED.company_name,
            segment                    = EXCLUDED.segment,
            country_code               = EXCLUDED.country_code,
            region                     = EXCLUDED.region,
            primary_address            = EXCLUDED.primary_address,
            secondary_address          = EXCLUDED.secondary_address,
            credit_limit               = EXCLUDED.credit_limit,
            payment_terms_days         = EXCLUDED.payment_terms_days,
            currency                   = EXCLUDED.currency,
            preferred_carrier          = EXCLUDED.preferred_carrier,
            contract_discount_pct      = EXCLUDED.contract_discount_pct,
            ordering_profile           = EXCLUDED.ordering_profile,
            seasonal_profile           = EXCLUDED.seasonal_profile,
            shutdown_months            = EXCLUDED.shutdown_months,
            accepts_deliveries_december = EXCLUDED.accepts_deliveries_december,
            active                     = EXCLUDED.active,
            onboarding_date            = EXCLUDED.onboarding_date
        """,
        rows,
    )
    log.info("customers: %d rows upserted", len(rows))


def _load_warehouses(cur):
    rows = json.loads((DATA_DIR / "warehouses.json").read_text())
    cur.executemany(
        """
        INSERT INTO dim.warehouses
            (code, name, city, country_code, role, inventory_share)
        VALUES
            (%(code)s, %(name)s, %(city)s, %(country_code)s, %(role)s, %(inventory_share)s)
        ON CONFLICT (code) DO UPDATE SET
            name            = EXCLUDED.name,
            city            = EXCLUDED.city,
            country_code    = EXCLUDED.country_code,
            role            = EXCLUDED.role,
            inventory_share = EXCLUDED.inventory_share
        """,
        rows,
    )
    log.info("warehouses: %d rows upserted", len(rows))


def load_dim():
    with _connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                _load_carriers(cur)
                _load_catalog(cur)
                _load_customers(cur)
                _load_warehouses(cur)
    log.info("dim load complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dim()
