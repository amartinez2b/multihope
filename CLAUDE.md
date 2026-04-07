# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Multihope** is a Medallion Architecture data pipeline built with PySpark. It processes customer data from a MySQL database through Bronze → Silver → Gold layers, with data quality validation at each stage using Databricks Labs DQX.

**Source database**: MySQL at `www.bigdataybi.com:3306`, database `fake` (Ecuadorian cleaning products distribution company).

## Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill in DB_USER and DB_PASSWORD
```

**Requirements**: Python >= 3.9, Java 8/11/17/21 (for PySpark).

## Common Commands

```bash
# Run tests
pytest tests/ -v

# Run a single test file
pytest tests/test_bronze_to_silver.py -v

# Run pipeline stages individually
python -m src.raw_to_bronze.customers_ingestion
python -m src.bronze_to_silver.customers_transform
python -m src.silver_to_gold.customers_aggregation
```

Notebooks in `notebooks/` are an alternative execution path and must be run in order: `00_precheck.ipynb` → `01_...` → `02_...` → `03_...`.

## Architecture

### Medallion Layers

```
MySQL (fake db)
    ↓ JDBC
[BRONZE]     /data/bronze/customers/       — raw Parquet, no transformation
    ↓ DQX validation
[SILVER]     /data/silver/customers/       — cleaned, validated records
[QUARANTINE] /data/quarantine/customers/   — records that failed error-level checks
    ↓ aggregation
[GOLD]       /data/gold/customers_summary/ — business-ready summaries by estado
```

### Source Code (`src/`)

- `utils/config_loader.py` — loads `config/database.yml` + `.env` credentials, builds JDBC URL
- `utils/spark_session.py` — SparkSession factory using `config/spark_config.yml`
- `raw_to_bronze/customers_ingestion.py` — MySQL → Parquet
- `bronze_to_silver/customers_transform.py` — DQX quality checks, deduplication, metadata columns
- `silver_to_gold/customers_aggregation.py` — aggregation by `estado`

### Data Quality (DQX)

Defined in `bronze_to_silver/customers_transform.py`:
- **Errors** (row goes to quarantine): `customer_id` not null, `nombre` not null/empty, `email` not null/empty
- **Warnings** (row passes but flagged): `identificacion` not empty, `estado` not null

Valid records get `_ingested_at` and `_source_layer` metadata columns added.

### Configuration

- `config/database.yml` — host, port, database name, JDBC driver class
- `config/spark_config.yml` — Spark session settings (memory, partitions, Maven packages)
- `.env` — `DB_USER` and `DB_PASSWORD` (never committed). **Never read this file** — it contains secrets.

### Data Catalog

`catalog/comercial/catalog.md` — human-readable data dictionary with ER diagram, column metadata, and example queries.  
`catalog/comercial/catalog.json` — machine-readable metadata for tables: `customers`, `products`, `sales`, `shops`.

### Tests

Tests in `tests/` use local PySpark with in-memory DataFrames — no MySQL connection needed. Each layer has its own test file mirroring the `src/` structure.

## MEMORY
@MEMORY.md