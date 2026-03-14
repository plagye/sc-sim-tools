# sc-sim-tools

Production-grade analytics pipeline for **FlowForm** — a fictional Polish B2B industrial valve manufacturer.

This repository is the **analytics pipeline** half of a two-repo system. It is paired with [sc-sim](../sc-sim/), the simulation engine that generates the source data. The simulator writes JSON files to a local output folder; this pipeline ingests them, transforms them through a medallion architecture, and delivers analytics-ready tables for Power BI and Excel.

```
sc-sim/          ← simulation engine (generates JSON source data)
sc-sim-tools/    ← this repo (ingests, transforms, serves analytics)
```

---

## Architecture

Data flows through three layers (Bronze → Silver → Gold), all stored in PostgreSQL:

```
sc-sim/output/  (JSON files — never copied, read directly)
      │
      ▼
┌─────────────────────────────────────┐
│  BRONZE  —  schema: raw             │
│  Raw JSON → Postgres, no transforms │
│  raw.events, dim.* master tables    │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  SILVER  —  schema: staging         │
│  Typed, deduplicated, DQ-flagged    │
│  23 staging.* tables                │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  GOLD  —  schema: mart              │
│  5 dims, 9 facts, 8 agg tables      │
│  export.* views for Power BI/Excel  │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  DAGSTER  —  Orchestration          │
│  14 assets, sensor, daily schedule  │
│  Web UI on port 3000                │
└─────────────────────────────────────┘
```

---

## Prerequisites

- Python 3.12
- PostgreSQL (local or remote — credentials in `.env`)
- [sc-sim](../sc-sim/) cloned as a sibling directory and run at least once to generate source data

---

## Setup

### 1. Clone both repos side by side

```bash
git clone <sc-sim-url>       # simulation engine
git clone <sc-sim-tools-url> # this repo
```

### 2. Create the virtual environment

```bash
cd sc-sim-tools
python3.12 -m venv venv
source venv/bin/activate
pip install -e .
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```env
DB_HOST=localhost
DB_NAME=supply_chain
DB_USER=your_user
DB_PASSWORD=your_password
DB_PORT=5432
DB_SSL=disable

# Path to sc-sim output folder (where the simulator writes JSON files)
DATA_DIR=/home/youruser/sc-sim/output
MASTER_DATA_DIR=/home/youruser/sc-sim/output/master
```

### 4. Create the PostgreSQL schemas

Run this once against your database:

```sql
CREATE SCHEMA dim;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA fact;
CREATE SCHEMA mart;
CREATE SCHEMA export;
```

---

## Running with Dagster (recommended)

Dagster orchestrates all pipeline scripts as assets and automatically detects new simulation data.

### Start the pipeline

Open **two terminals**, both in the project root:

```bash
# Terminal 1 — web UI at http://localhost:3000
cd /home/youruser/sc-sim-tools
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster-webserver -w workspace.yaml

# Terminal 2 — daemon (runs the sensor and schedule)
cd /home/youruser/sc-sim-tools
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster-daemon run -w workspace.yaml
```

### Enable the sensor

The sensor is paused by default. Enable it once:

**Option A — Web UI:**
1. Open http://localhost:3000
2. Click **Automation** in the left sidebar
3. Find `sim_data_sensor` → toggle to **Running**

**Option B — CLI:**
```bash
cd /home/youruser/sc-sim-tools
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster sensor start sim_data_sensor -w workspace.yaml
```

### Automatic operation

Once the sensor is running, the pipeline is fully automatic:

1. Run `sc-sim` to generate new simulation data in `DATA_DIR`
2. The sensor scans `DATA_DIR` every 30 seconds for new files
3. When new files are detected, `pipeline_job` triggers automatically
4. Bronze → Silver → Gold runs end-to-end without any manual steps
5. Results are available immediately in the `mart.*` and `export.*` schemas

### Dagster UI overview

| Section | What it does |
|---|---|
| **Asset Catalog** | Visual graph of all 14 assets. Click any asset to see its last run, row count metadata, and logs. |
| **Runs** | Full history of every pipeline execution with per-asset timing and logs. |
| **Automation** | Enable/disable `sim_data_sensor` (filesystem trigger) and `daily_full_run` (midnight schedule). |
| **Jobs** | Manually launch `pipeline_job` (full run) or `agg_refresh_job` (aggregates only) via the Launchpad. |

---

## Manual pipeline runs (without Dagster UI)

All commands must be run from the project root with `DAGSTER_HOME` set:

```bash
cd /home/youruser/sc-sim-tools

# Full pipeline — all 14 assets in dependency order
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster asset materialize -m sc_sim_pipeline --select '*'

# Gold layer only — skip bronze/silver (use when source data hasn't changed)
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster asset materialize -m sc_sim_pipeline \
  --select "gold_dimensions_asset gold_fact_orders_asset gold_fact_financial_tms_asset gold_fact_inventory_asset gold_aggregates_asset gold_exports_asset"

# Aggregates only — fastest refresh, just recomputes mart.agg_* tables
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster asset materialize -m sc_sim_pipeline \
  --select "gold_aggregates_asset gold_exports_asset"

# Single asset — useful for debugging
DAGSTER_HOME=$(pwd)/dagster_home venv/bin/dagster asset materialize -m sc_sim_pipeline \
  --select raw_events
```

---

## Running scripts directly (standalone, no Dagster)

Each script is runnable standalone for debugging. Run from the `src/` directory:

```bash
cd src

# Bronze
python event_aggregator.py   # ingest new JSON files → raw.events
python load_master.py        # upsert master/reference data → dim.*

# Silver
python stg_orders.py
python stg_erp_batch1.py
python stg_erp_batch2.py
python stg_erp_batch3.py
python stg_tms.py
python stg_adm_forecast.py

# Gold
python gold_dimensions.py
python gold_fact_orders.py
python gold_fact_inventory_production.py
python gold_fact_financial_tms.py
python gold_aggregates.py
python gold_exports.py
```

All scripts are idempotent — safe to re-run at any time.

---

## Project structure

```
sc-sim-tools/
├── src/
│   ├── db.py                          # shared psycopg connection factory
│   ├── event_aggregator.py            # Bronze: JSON → raw.events
│   ├── load_master.py                 # Bronze: master data → dim.*
│   ├── stg_orders.py                  # Silver: orders + order lines
│   ├── stg_erp_batch1.py              # Silver: exchange rates, inventory, credit, production
│   ├── stg_erp_batch2.py              # Silver: payments, master data changes, cancellations
│   ├── stg_erp_batch3.py              # Silver: supply disruptions
│   ├── stg_tms.py                     # Silver: loads, carrier events, returns
│   ├── stg_adm_forecast.py            # Silver: demand plans, forecasts, signals
│   ├── gold_dimensions.py             # Gold: SCD2 dimension tables
│   ├── gold_fact_orders.py            # Gold: fact_order_lines (with TMS shipped-qty update)
│   ├── gold_fact_inventory_production.py  # Gold: fact_inventory_daily, fact_production
│   ├── gold_fact_financial_tms.py     # Gold: fact_shipments, fact_payments, fact_returns
│   ├── gold_aggregates.py             # Gold: all mart.agg_* tables
│   ├── gold_exports.py                # Gold: export.* views for Power BI / Excel
│   └── sc_sim_pipeline/               # Dagster package
│       ├── __init__.py                # Definitions (assets, resources, sensor, jobs)
│       ├── assets/
│       │   ├── bronze.py              # raw_events, dim_tables assets
│       │   ├── silver.py              # 6 silver assets
│       │   └── gold.py                # 6 gold assets
│       ├── resources/
│       │   └── postgres.py            # PostgresResource
│       ├── sensors/
│       │   └── sim_sensor.py          # sim_data_sensor (watches DATA_DIR)
│       ├── jobs/
│       │   └── pipeline_job.py        # pipeline_job, agg_refresh_job
│       └── schedules/
│           └── schedules.py           # daily_full_run (midnight, Europe/Warsaw)
├── dagster_home/                      # Dagster run storage and logs
├── dagster.yaml                       # Dagster instance config
├── workspace.yaml                     # Dagster workspace (points to sc_sim_pipeline)
├── pyproject.toml                     # package config + dependencies
├── requirements.txt                   # pinned dependencies
└── .env                               # DB credentials + DATA_DIR (not committed)
```

---

## Database schemas

| Schema | Contents |
|---|---|
| `dim` | Master/reference tables: `carriers`, `catalog`, `customers`, `warehouses` |
| `raw` | Bronze layer: `events` (all raw JSON), `ingested_files` (idempotency tracking) |
| `staging` | Silver layer: 23 typed, deduplicated staging tables with `_dq_flags` lineage |
| `mart` | Gold layer: 5 SCD2 dimensions, 9 fact tables, 8 pre-aggregated metric tables |
| `export` | Flat views joining mart tables — direct consumption by Power BI / Excel |

---

## Downstream consumers

### Power BI
Connect via DirectQuery or Import to the `mart.*` schema. Key report pages:

| Report | Primary tables |
|---|---|
| Executive Overview | `agg_order_metrics_daily`, `agg_cash_flow_monthly` |
| Customer Scorecard | `agg_customer_scorecard`, `dim_customer` |
| Inventory Health | `agg_inventory_health`, `fact_inventory_daily` |
| Carrier Performance | `agg_carrier_scorecard`, `fact_shipments` |
| Demand Accuracy | `agg_demand_accuracy` |
| Returns & Quality | `fact_returns`, `agg_production_quality` |
| Finance / AR | `agg_cash_flow_monthly`, `fact_payments`, `fact_credit_events` |

### Excel
Use the `export.*` schema — pre-joined flat views requiring no joins in Power Query:

- `export.orders_with_customer_and_sku`
- `export.inventory_daily_flat`
- `export.carrier_performance_flat`
- `export.demand_accuracy_flat`
- `export.customer_ar_flat`

---

## Key design decisions

**Idempotency everywhere** — all scripts are safe to re-run. Bronze skips files already in `raw.ingested_files`. Silver uses `raw_event_id` watermarks. Gold uses `INSERT ... ON CONFLICT DO UPDATE`.

**Shipped quantities from TMS** — `customer_orders.json` records only the creation-time snapshot (all `qty_shipped = 0`). Actual shipment data lives in TMS load files. `gold_fact_orders.py` runs a second-pass UPDATE that marks order lines as shipped for any order appearing in delivered/pod_received loads.

**No ORM** — all SQL lives in `.sql` strings inside pipeline scripts. `psycopg` v3 throughout.

**DQ lineage** — every staging table has a `_dq_flags TEXT[]` column. Rows with critical missing fields (null `order_id`, null `customer_id`) are rejected to `staging.dq_rejects` with a reason code.
