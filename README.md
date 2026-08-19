# AdPulse — Campaign Analytics ETL Pipeline & Data Warehouse

AdPulse turns messy, multi-source ad-campaign data into an analytics-ready
star schema in PostgreSQL. It's a small but complete ETL/ELT pipeline built
to demonstrate practical data engineering: extraction from heterogeneous
sources, staged loading, pandas-based transformation, dimensional modeling,
data-quality validation, orchestration, and containerization.

## Why this project exists

Marketing teams pull campaign performance from several channels (search,
social, display, email), each exporting data in a different shape — one CSV
export here, one JSON "API-style" feed there. AdPulse ingests both, lands
them raw, cleans and models them into a proper fact/dimension warehouse, and
computes standard marketing metrics (CTR, CPC, CPA, ROAS) so analysts can
just write `SELECT`s instead of re-cleaning data every time.

## Architecture

```
 ┌────────────┐     ┌───────────────┐     ┌───────────────┐     ┌──────────────┐     ┌────────────┐
 │  Sources   │ --> │  Extract      │ --> │  Load: staging│ --> │  Transform   │ --> │  Warehouse │
 │ CSV + JSON │     │ (extract.py)  │     │ (load_staging)│     │ (transform.py│     │ star schema│
 └────────────┘     └───────────────┘     └───────────────┘     └──────────────┘     └────────────┘
                            │                                                                │
                            ▼                                                                ▼
                     data/raw/*.csv,json                                             validate.py
                     (optionally mirrored                                     (row-count checks, null
                      to an S3 bucket)                                        checks, dup checks, logged
                                                                                 data-quality report)
```

**Design: batch, not streaming.** Campaign performance data (impressions,
clicks, spend, conversions) is reported by ad platforms on a daily
aggregate basis, not as an event stream — there's no per-click event feed to
consume in real time in this context, and daily/hourly batch loads are the
industry-standard cadence for marketing reporting. A streaming design would
add operational complexity (Kafka/Kinesis, exactly-once semantics,
watermarking) with no analytical benefit here. If a future requirement
needed near-real-time spend alerts, the extract layer could be swapped for
a consumer without changing the warehouse model.

### Layers

1. **Extract** (`src/extract.py`) — reads the two source feeds (CSV export,
   JSON feed) from `data/raw/`. Optionally uploads raw files to an S3
   bucket first, to simulate landing data from a real ingestion pipeline
   (`--use-s3` flag; falls back to local-only if AWS isn't configured).
2. **Load (raw → staging)** (`src/load_staging.py`) — loads the raw files
   as-is into `staging` schema tables in Postgres. No cleaning here; this
   is the "landing zone," so bad data is visible and auditable.
3. **Transform** (`src/transform.py`) — pandas: dedupes, fixes types,
   handles nulls, standardizes channel/campaign names, derives metrics
   (CTR, CPC, CPA, ROAS), writes a clean intermediate Parquet file to
   `data/processed/`.
4. **Load (warehouse)** (`src/load_warehouse.py`) — loads the clean data
   into a dimensional (star) schema: `fact_campaign_performance` plus
   `dim_date`, `dim_campaign`, `dim_channel`, `dim_geography`.
5. **Validate** (`src/validate.py`) — data-quality gate: compares row
   counts between staging and warehouse, checks for nulls in required
   columns, flags duplicate fact rows, flags metric outliers (e.g.
   CTR > 100%), and writes a timestamped report to `logs/`.
6. **Orchestrate** (`dags/campaign_pipeline_dag.py`) — an Airflow DAG that
   chains extract → load_staging → transform → load_warehouse → validate
   with retries and clear task dependencies. Runs daily.
7. **Containerize** (`docker-compose.yml`) — spins up Postgres (with the
   schema auto-applied on first boot) and the pipeline runner in one
   command.

## Repo structure

```
adpulse/
├── data/
│   ├── raw/                        # landed source files (CSV + JSON)
│   └── processed/                  # cleaned Parquet output from transform.py
├── src/
│   ├── config.py                   # env-driven config (DB creds, paths, S3)
│   ├── db.py                       # shared Postgres connection helper
│   ├── generate_sample_data.py     # creates realistic synthetic source data
│   ├── extract.py
│   ├── load_staging.py
│   ├── transform.py
│   ├── load_warehouse.py
│   ├── validate.py
│   └── run_pipeline.py             # runs all stages end-to-end, for local use
├── sql/
│   ├── staging_schema.sql          # raw landing tables
│   └── warehouse_schema.sql        # star schema DDL (facts + dims)
├── dags/
│   └── campaign_pipeline_dag.py    # Airflow DAG
├── tests/
│   └── test_transform.py           # unit tests for the transform/metric logic
├── logs/                           # data-quality reports land here
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Data model

**Grain of the fact table:** one row per (campaign, channel, date,
geography).

```
dim_date            dim_campaign          dim_channel        dim_geography
-----------         -----------------     ---------------    ---------------
date_key (PK)        campaign_key (PK)     channel_key (PK)   geo_key (PK)
full_date            campaign_id           channel_name       country
day_of_week           campaign_name         channel_type       region
month                objective                                   city
quarter              start_date
year                 end_date
is_weekend

                         fact_campaign_performance
                         --------------------------
                         fact_key (PK)
                         date_key      (FK -> dim_date)
                         campaign_key  (FK -> dim_campaign)
                         channel_key   (FK -> dim_channel)
                         geo_key       (FK -> dim_geography)
                         impressions
                         clicks
                         spend
                         conversions
                         revenue
                         ctr            -- clicks / impressions
                         cpc            -- spend / clicks
                         cpa            -- spend / conversions
                         roas           -- revenue / spend
                         load_ts
```

This is a classic Kimball star schema: denormalized dimensions for fast,
simple analytical joins, one clearly-defined fact table grain, and derived
metrics pre-computed at load time so BI tools don't need to recompute them
per query.

## Getting started

### Option A — Docker (fastest)

```bash
cp .env.example .env
docker compose up --build
```

This starts Postgres, applies both schema files automatically, and runs
the full pipeline once against generated sample data.

### Option B — Local Python

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # then edit with your local Postgres creds

# 1. create a local Postgres DB matching .env, then apply schemas:
psql -f sql/staging_schema.sql
psql -f sql/warehouse_schema.sql

# 2. generate sample source data (or drop your own into data/raw/)
python src/generate_sample_data.py

# 3. run the full pipeline
python src/run_pipeline.py
```

### Running individual stages

```bash
python -m src.extract
python -m src.load_staging
python -m src.transform
python -m src.load_warehouse
python -m src.validate
```

### Running on Airflow

Copy `dags/campaign_pipeline_dag.py` into your Airflow `dags/` folder and
set the `AIRFLOW_HOME`/connection env vars to match `.env`. The DAG runs
daily, retries each task twice on failure, and fails loudly (rather than
loading partial data) if `validate.py` finds a data-quality issue.

## Example analytical queries

```sql
-- ROAS by channel, last 30 days
select c.channel_name,
       round(sum(f.revenue) / nullif(sum(f.spend), 0), 2) as roas
from fact_campaign_performance f
join dim_channel c on c.channel_key = f.channel_key
join dim_date d on d.date_key = f.date_key
where d.full_date >= current_date - interval '30 days'
group by c.channel_name
order by roas desc;

-- Best-performing campaigns by CPA
select cm.campaign_name, avg(f.cpa) as avg_cpa, sum(f.conversions) as conversions
from fact_campaign_performance f
join dim_campaign cm on cm.campaign_key = f.campaign_key
group by cm.campaign_name
having sum(f.conversions) > 0
order by avg_cpa asc
limit 10;
```

## Tech stack

- **Python** — pandas, NumPy for cleaning/transformation
- **PostgreSQL** — staging schema + dimensional warehouse
- **Parquet** — intermediate storage between transform and load
- **Airflow** (optional) — DAG orchestration
- **Docker / docker-compose** — containerized Postgres + pipeline
- **boto3** (optional) — S3 landing zone for raw files

## Notes / possible extensions

- Swap `load_warehouse.py`'s upsert logic for SCD Type 2 on `dim_campaign`
  if campaign metadata (objective, dates) needs to be tracked historically.
- Add a `dbt` layer on top of the staging tables instead of hand-written
  transform SQL, if the project grows.
- Add a lightweight BI layer (Metabase/Superset) pointed at the warehouse
  for a demo dashboard.
