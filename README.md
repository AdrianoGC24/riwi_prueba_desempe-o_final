# DataMart S.A.S. — ETL Pipeline with Apache Airflow & PostgreSQL Data Warehouse

**Performance Test — Data Engineering Cohort 7**  
**Author:** Adriano de Jesús González Cera

---

## Overview

End-to-end ETL pipeline that ingests two e-commerce CSV datasets, transforms and validates them applying DataMart's business rules, and loads a star schema into a PostgreSQL analytical repository — all orchestrated with Apache Airflow 3.1.5 running inside Docker.

```
data.csv  ──┐
             ├─► [Extract] ──► [Transform + Validate] ──► [Load] ──► PostgreSQL DW
online_retail.csv ──┘                   │
                               FastAPI Catalog API (optional enrichment)
```

**Data Warehouse tables loaded:**

| Table | Rows |
|---|---|
| `dim_date` | 305 |
| `dim_products` | 3,828 |
| `dim_customers` | 4,372 |
| `fact_transactions` | 534,127 |
| `fact_daily_product_metrics` | 278,011 |
| `audit_rejected_records` | 2,512 |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git
- The two Kaggle CSV files placed inside `data_sources/` (see below)

---

## Quick Start (under 10 minutes)

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd prueba_desempeño_final
```

### 2. Place the data files

Download the two datasets and put them here:

```
data_sources/
├── data.csv              # https://www.kaggle.com/datasets/carrie1/ecommerce-data
└── online_retail.csv     # https://www.kaggle.com/datasets/thedevastator/online-retail-transaction-dataset
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

The default values in `.env.example` work out of the box. You only need to edit `.env` if you want to change passwords or ports.

### 4. Build and start all services

> **⚠️ Windows-only workaround:** The folder name contains `ñ`, which breaks Docker BuildKit's session handshake on Windows. Always build separately using these two commands — **never use `docker compose up --build`**:

**Windows (PowerShell):**
```powershell
$env:DOCKER_BUILDKIT=0; $env:COMPOSE_DOCKER_CLI_BUILD=0; docker compose build
docker compose up -d
```

**Linux / macOS:**
```bash
docker compose up -d --build
```

### 5. Wait for initialization (~60 seconds)

Check that all 7 containers are healthy:

```bash
docker compose ps
```

Expected output — all services should show `running` or `healthy`:

| Service | Port |
|---|---|
| `datamart_airflow_apiserver` | 8080 |
| `datamart_analytical_dw` | 5433 |
| `datamart_catalog_api` | 8000 |
| `datamart_airflow_scheduler` | — |
| `datamart_airflow_dag_processor` | — |
| `datamart_airflow_triggerer` | — |
| `datamart_airflow_metadata_db` | — |

---

## Verifying Connections and Variables

All Airflow Connections and Variables are configured **automatically** from the `.env` file — no manual UI steps required.

To verify from the terminal:

```bash
# Check the postgres_dw connection exists
docker exec datamart_airflow_apiserver airflow connections get postgres_dw

# Check all Variables loaded correctly
docker exec datamart_airflow_apiserver airflow variables list
```

Expected variables:

| Variable | Default value |
|---|---|
| `data_sources_dir` | `/opt/airflow/data_sources` |
| `staging_dir` | `/opt/airflow/logs/staging` |
| `use_catalog_api` | `true` |
| `batch_size` | `50000` |

---

## Running the Pipeline

### Option A — Airflow UI (recommended)

1. Open **http://localhost:8080** → user: `airflow` / password: `airflow`
2. Find the DAG `datamart_sales_etl_pipeline` and toggle it **ON**
3. Click **▶ Trigger DAG**
4. Watch the 4 tasks execute in order:
   - `extract_daily_sales_csv` ──┐ (parallel)
   - `extract_historical_sales_csv` ──┘
   - `transform_validate_and_build_star_schema`
   - `load_star_schema_to_postgres_dw`

### Option B — CLI

```bash
docker exec datamart_airflow_apiserver \
  airflow dags trigger datamart_sales_etl_pipeline
```

---

## Verifying Data in the Analytical Repository

Connect to the Data Warehouse:

```bash
docker exec -it datamart_analytical_dw \
  psql -U dw_admin -d datamart_dw
```

Or from any PostgreSQL client (e.g., DBeaver, TablePlus):
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `datamart_dw`
- **User:** `dw_admin`
- **Password:** `dw_secure_password_2026`

Quick validation query:

```sql
SELECT
    'dim_date'                  AS table_name, COUNT(*) AS rows FROM dim_date
UNION ALL SELECT 'dim_products',               COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_customers',              COUNT(*) FROM dim_customers
UNION ALL SELECT 'fact_transactions',          COUNT(*) FROM fact_transactions
UNION ALL SELECT 'fact_daily_product_metrics', COUNT(*) FROM fact_daily_product_metrics
UNION ALL SELECT 'audit_rejected_records',     COUNT(*) FROM audit_rejected_records;
```

---

## Catalog API

The internal product catalog API is available at **http://localhost:8000**.

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /products?page=1&limit=100` | Paginated product listing |
| `GET /products/{stock_code}` | Single product by code |
| `GET /docs` | Interactive Swagger UI |

---

## Business Validation Queries

All 7 business questions from Section 7 of the spec have SQL files in `sql_validations/`:

| File | Question |
|---|---|
| `q1_monthly_net_sales_evolution.sql` | Monthly evolution of net sales |
| `q2_category_revenue_and_return_rate.sql` | Revenue and return rate by category |
| `q3_top10_products_by_net_revenue_and_return_rate.sql` | Top 10 products by revenue and returns |
| `q4_countries_transactions_and_avg_ticket.sql` | Countries by volume and average ticket |
| `q5_identified_vs_unidentified_customers.sql` | Identified vs. unidentified customer behavior |
| `q6_inconsistent_descriptions_and_unique_products.sql` | Products with inconsistent descriptions |
| `q7_business_recommendation.sql` | Data-backed business recommendation |

Run any query directly:

```bash
docker exec -i datamart_analytical_dw \
  psql -U dw_admin -d datamart_dw \
  < sql_validations/q1_monthly_net_sales_evolution.sql
```

---

## Project Structure

```
prueba_desempeño_final/
├── dags/
│   ├── dag_etl_datamart.py          # Airflow DAG — pipeline orchestration
│   └── scripts/
│       ├── extract.py               # Task 1 & 2: CSV → Parquet (staging)
│       ├── transform.py             # Task 3: validation, dedup, star schema build
│       └── load.py                  # Task 4: bulk upsert into PostgreSQL DW
├── catalog_api/
│   ├── main.py                      # FastAPI catalog service
│   ├── products_seed.json           # 3,828 products seed data
│   ├── requirements.txt
│   └── Dockerfile
├── database_init/
│   └── init_schema.sql              # Star schema DDL (auto-executed on DW startup)
├── sql_validations/
│   ├── q1_monthly_net_sales_evolution.sql
│   ├── q2_category_revenue_and_return_rate.sql
│   ├── q3_top10_products_by_net_revenue_and_return_rate.sql
│   ├── q4_countries_transactions_and_avg_ticket.sql
│   ├── q5_identified_vs_unidentified_customers.sql
│   ├── q6_inconsistent_descriptions_and_unique_products.sql
│   └── q7_business_recommendation.sql
├── exploratory_data_analysis/
│   └── eda.py                       # EDA script — data quality analysis
├── data_sources/                    # (git-ignored) Place CSV files here
├── logs/                            # (git-ignored) Airflow logs + Parquet staging
├── docker-compose.yaml
├── Dockerfile.airflow
├── .env.example
├── .gitignore
├── .dockerignore
├── Business_decision.md             # Technical decisions document
└── README.md
```

---

## Stopping the Environment

```bash
docker compose down          # Stop containers (data persists in volumes)
docker compose down -v       # Stop and delete all volumes (full reset)
```
