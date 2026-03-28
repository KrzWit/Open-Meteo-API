# Open-Meteo API to PostgreSQL ETL (Portfolio Project)

End-to-end data engineering project that ingests hourly weather data from Open-Meteo for multiple cities, stores raw API responses for traceability, transforms them into a normalized model, and loads them into PostgreSQL using idempotent upserts.

The official analytics layer is delivered as Jupyter dashboards.

## Why this project matters

This project simulates a realistic analytics ingestion pipeline where:

- source data is external and semi-structured (JSON API)
- ingestion must be repeatable and safe to rerun
- raw and curated layers are both required
- stakeholders need quick visibility through dashboards

It demonstrates practical ETL engineering patterns, not just one-off scripting.

## Core features

- Extraction from Open-Meteo API for a configurable list of cities
- Raw payload persistence in PostgreSQL for auditing and reprocessing
- Data transformation to a clean, hourly normalized schema
- Idempotent final-table loading with PostgreSQL ON CONFLICT upsert
- Environment-driven configuration with .env
- Structured logging and robust exception handling
- Unit tests for transformation logic
- Docker Compose setup for one-command local execution
- Jupyter dashboards for data quality and trend inspection

## Tech stack

- Python 3.11+
- requests
- pandas
- SQLAlchemy
- psycopg2-binary
- python-dotenv
- matplotlib
- PostgreSQL
- Docker + Docker Compose
- pytest
- Jupyter Notebook

## Architecture overview

Pipeline modules are separated by responsibility:

- app/config.py: reads and validates runtime settings
- app/extractor.py: pulls hourly weather payloads from Open-Meteo
- app/transformer.py: converts payloads into normalized pandas DataFrames
- app/loader.py: writes raw JSON and upserts final hourly records
- app/pipeline.py: orchestrates end-to-end ETL execution
- notebooks/weather_dashboard.ipynb: detailed analytics dashboard
- notebooks/executive_summary.ipynb: presentation-focused summary dashboard

Execution entry points:

- main.py: run ETL
- notebooks/weather_dashboard.ipynb: exploratory dashboard
- notebooks/executive_summary.ipynb: executive portfolio dashboard

### Architecture diagram

```mermaid
flowchart LR
    A[Open-Meteo API] --> B[Extractor]
    B --> C[Raw Layer: raw_weather_data]
    B --> D[Transformer]
    D --> E[Loader with UPSERT]
    E --> F[Curated Layer: weather_hourly]
    F --> G[Notebook Dashboard: weather_dashboard.ipynb]
    F --> H[Notebook Dashboard: executive_summary.ipynb]
```

## Data model

### 1) raw_weather_data

- id (primary key)
- city
- latitude
- longitude
- ingestion_timestamp
- raw_json

Purpose:

- immutable ingestion history
- auditability
- ability to re-transform without API re-fetch

### 2) weather_hourly

- id (primary key)
- city
- latitude
- longitude
- timestamp
- temperature_2m
- relative_humidity_2m
- precipitation
- wind_speed_10m
- load_timestamp
- unique(city, timestamp)

Purpose:

- analytics-ready hourly fact table
- stable schema for BI/dashboard usage

## Idempotency strategy

The weather_hourly table enforces unique(city, timestamp).

Loader logic uses PostgreSQL ON CONFLICT DO UPDATE, which means:

- first run inserts new rows
- subsequent runs for the same city/timestamp update existing rows
- duplicates are not created in the curated layer

Note: raw_weather_data intentionally keeps every ingestion event, so its row count grows with each run.

## Jupyter notebook dashboards

Notebook files:

- notebooks/weather_dashboard.ipynb
- notebooks/executive_summary.ipynb

weather_dashboard.ipynb contains:

- ETL KPI panel (raw rows, final rows, city count, min/max timestamp)
- temperature trend per city
- precipitation aggregation per city
- humidity distribution stats
- wind speed trend per city
- latest records preview

executive_summary.ipynb contains 4 presentation charts:

- KPI summary table
- temperature trend by city
- total precipitation by city
- average wind speed by city

How to run in VS Code:

1. Start database and ETL first:

```bash
docker compose up --build
```

2. Open notebooks/weather_dashboard.ipynb and run all cells from top to bottom.

3. Open notebooks/executive_summary.ipynb and run all cells from top to bottom.

4. If your notebook runs on host (not in Docker), use:

```powershell
$env:NOTEBOOK_DATABASE_URL="postgresql+psycopg2://etl_user:etl_password@localhost:5432/weather_etl"
```

The notebook already auto-switches from @db to @localhost when needed.

## Project structure

```text
Open-Meteo-API/
  app/
    __init__.py
    config.py
    database.py
    models.py
    extractor.py
    transformer.py
    loader.py
    pipeline.py
  tests/
    test_transformer.py
  notebooks/
    weather_dashboard.ipynb
    executive_summary.ipynb
  main.py
  requirements.txt
  pytest.ini
  Dockerfile
  docker-compose.yml
  .env.example
  README.md
```

## Quick start (recommended: Docker)

1. Copy environment template:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

2. Build and run:

```bash
docker compose up --build
```

This starts:

- PostgreSQL database
- ETL runner service

3. Open notebook dashboards and run cells.

## Useful commands

Run ETL again (incremental rerun test):

```bash
docker compose run --rm etl
```

Run tests inside container:

```bash
docker compose run --rm etl pytest -q
```

Stop services:

```bash
docker compose down
```

Stop and remove DB volume:

```bash
docker compose down -v
```

## Local run (without Docker)

1. Create Python 3.11+ virtual environment
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables (.env)
4. Run ETL:

```bash
python main.py
```

5. Open notebook dashboards and run cells.

## Testing

- tests/test_transformer.py validates transformation behavior:
  - column mapping and typing
  - multi-city concatenation
  - empty payload handling

Run:

```bash
pytest -q
```

## Observability and reliability

- Structured logs (timestamp | level | module | message)
- API call failures logged per city
- Database transaction rollback on exceptions
- Graceful handling when source payloads are empty

## Portfolio talking points

When presenting this project, emphasize:

- clear layered modeling (raw vs curated)
- safe rerun behavior via upserts and uniqueness constraints
- modular code organization with explicit responsibilities
- reproducible environment with Docker Compose
- direct business visibility through notebook dashboards

## Possible next improvements

- Workflow scheduling (Airflow/Prefect)
- Data quality checks and alerts
- Incremental extraction window control
- Alembic migrations
- CI pipeline with automated tests
- Materialized aggregates for faster dashboard queries
