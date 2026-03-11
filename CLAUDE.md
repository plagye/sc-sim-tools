# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**sc-sim-tools** is a data pipeline and analytics toolset for supply chain simulation data. The source data lives in `data/` (gitignored, generated externally by a separate simulator project) and represents a fictional industrial valve manufacturer (FlowForm) operating in Poland.

The project scope is a **raw → staging → fact/mart pipeline** backed by PostgreSQL, with potential additions of a dashboard and CRUD interfaces down the line.

A Python 3.12 virtual environment (`venv/`) is present. Install dependencies into it as needed.

## Coding Standards

- **Write the minimum code required.** Do not add anything not explicitly asked for — no extra error handling, no extra logging, no helper abstractions, no configuration options, no type annotations beyond what's requested.
- **If something is unclear, ask** before writing code. A short clarifying question is better than a wrong assumption.
- **No speculative generalization.** Don't design for future requirements that haven't been stated.

## Source Data

All source data is under `data/` (JSON files, gitignored). Five domains:

- **`data/master/`** — static reference data: `catalog.json` (~800 valve SKUs), `customers.json` (~50 B2B customers), `warehouses.json` (2 DCs: W01 Katowice, W02 Gdańsk), `carriers.json` (5 carriers), `initial_inventory.json`, `initial_customer_balances.json`
- **`data/adm/`** — demand plans and inventory targets (monthly folders) + `demand_signals/`
- **`data/forecast/`** — ML demand forecasts (monthly folders, 6-month horizons)
- **`data/erp/`** — daily folders: `customer_orders.json`, `exchange_rates.json`
- **`data/tms/`** — daily folders: `loads_XX.json` (multiple per day), `carrier_events.json`

Key ID conventions: customers `CUST-0001..0050`, orders `ORD-2026-XXXXX`, loads/events use UUIDs. SKU codes encode valve type, DN, material, pressure class, connection type, and actuation method.

## Pipeline Architecture (planned)

```
data/ (JSON source)
  └─► raw       (load JSON as-is into Postgres)
  └─► staging   (clean, type-cast, deduplicate)
  └─► fact/mart (business-level aggregations and metrics)
```
