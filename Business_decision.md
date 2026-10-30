# Technical Decisions Document

**Project:** DataMart S.A.S. — ETL Pipeline with Apache Airflow & PostgreSQL Data Warehouse  
**Author:** Adriano de Jesús González Cera  
**Cohort:** Data Engineering — Cohort 7

---

## 1. Exploratory Data Analysis (EDA) Findings

Before writing a single line of ETL code, an EDA script (`exploratory_data_analysis/eda.py`) was run against both raw CSVs. The key findings that drove all subsequent decisions:

| Finding | Value |
|---|---|
| Total rows (each file) | 541,909 |
| Unique invoices | 25,900 |
| Date range | 2010-12-01 → 2011-12-09 |
| Rows with null `CustomerID` | 135,080 (24.93%) |
| Rows with `UnitPrice = 0` | 2,515 |
| Rows with negative `Quantity` (returns) | 10,624 |
| Products with multiple description variants | 650 |
| Rows with null `Description` | 1,454 |

**Critical discovery:** `data.csv` and `online_retail.csv` contain **identical invoices** for the same date range. The only structural difference is that `online_retail.csv` contains an extra `index` column. This shaped the deduplication strategy (see Section 3).

---

## 2. Analytical Repository Design — Star Schema

### Decision: Star Schema over a flat table

A star schema was chosen over a single denormalized table because:

1. **Business question alignment** — The spec's 7 questions require grouping by date (monthly), product category, customer type, and country — all natural dimensions.
2. **BI-ready** — `dim_date` means any BI tool (Metabase, Tableau, Power BI) can immediately do time intelligence without preprocessing.
3. **Separation of concerns** — Sales and returns live in the same `fact_transactions` table with a `transaction_type` column (`SALE`/`RETURN`), satisfying the spec's requirement to separate them while keeping them queryable together.

### Tables created

| Table | Type | Description |
|---|---|---|
| `dim_date` | Dimension | Calendar table with year, quarter, month, day, weekend flag |
| `dim_products` | Dimension | Consolidated product catalog enriched with category and description variant count |
| `dim_customers` | Dimension | All customer IDs including the synthetic `UNIDENTIFIED` customer |
| `fact_transactions` | Fact (grain: invoice line) | Every validated transaction with gross amount and type |
| `fact_daily_product_metrics` | Fact (aggregate) | Pre-computed daily net revenue per product — enables fast BI queries |
| `audit_rejected_records` | Audit | All rejected records with the rejection reason and source file |

### Why two fact tables?

- `fact_transactions` gives full transaction-level granularity (needed for per-invoice analysis, customer behavior, etc.).
- `fact_daily_product_metrics` is a pre-aggregated summary that makes the most common analytical queries (monthly revenue, return rates by category) dramatically faster — no need to `SUM` 534,000 rows every time.

---

## 3. Ambiguous Cases — Decisions Taken

### 3.1 Two CSV files with identical data

**Problem:** Both `data.csv` and `online_retail.csv` cover the same date range and invoices. How to handle duplicates between sources?

**Decision:** Concatenate both files vertically, then **deduplicate by composite business key**: `(InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)`. This approach:
- Does not give priority to either source (both are equally valid).
- Detects true duplicates regardless of which file they came from.
- Preserves any record that is genuinely unique in one source.
- Removes the spurious `index` column from `online_retail.csv` during extract.

**Alternative considered:** Give priority to `data.csv` (the "daily" source). Rejected because if a record exists in only `online_retail.csv`, it would be silently dropped.

### 3.2 Transactions without CustomerID (24.93% of data)

**Problem:** 135,080 rows have no `CustomerID`. The business has no official rule.

**Decision:** **Include them** — tag them with the synthetic ID `'UNIDENTIFIED'` in `dim_customers` with `is_identified = FALSE`.

**Justification:**
- Excluding 25% of transactions would make revenue figures unreliable.
- Flagging them as unidentified preserves the data while allowing the business to filter them out if needed.
- This directly enables Business Question 5 (behavior comparison between identified and unidentified customers).
- The `dim_customers` table stores `is_identified` as a boolean, making segmentation queries trivial.

### 3.3 Products with multiple description variants

**Problem:** The same `StockCode` appears with different descriptions across rows (e.g., `CANDLE HOLDER WHITE`, `Candle Holder White`, `candle holder white`).

**Decision:** **Statistical mode** — for each `StockCode`, the most frequently occurring description (after uppercasing and stripping) is chosen as the `canonical_description`. The number of distinct variants found is stored in `dim_products.raw_variation_count` for traceability (directly answers Business Question 6).

**Justification:** The mode is the description the data itself "votes for" — no manual mapping required and it scales to any number of products.

### 3.4 CustomerID `.0` suffix

**Problem:** CustomerID values appear as `17850.0`, `13047.0` etc. because pandas reads them as floats when nulls are present.

**Decision:** Convert via `pd.to_numeric(...).astype('Int64').astype(str)` to produce clean integer strings (`17850`, `13047`). This was confirmed by research — no country uses `.0` in a real customer identifier; the suffix is purely a pandas type-coercion artifact.

### 3.5 Negative UnitPrice / UnitPrice = 0

**Decision:** **Reject** — these rows are stored in `audit_rejected_records` with reason `PRICE_ZERO_OR_NEGATIVE`. A zero or negative price in a sales transaction is not physically meaningful.

**Impact:** 2,517 records rejected (~0.46% of total).

### 3.6 Negative Quantity (returns)

**Decision:** **Keep and classify** — rows with `Quantity <= 0` are kept as `transaction_type = 'RETURN'`. They are not rejected because they represent real business events (returns and adjustments) that must be tracked for net revenue calculations.

---

## 4. Idempotency Guarantee

**Requirement (Section 6):** Running the DAG twice on the same day with the same data must produce exactly the same result in the analytical repository.

**How it is guaranteed — two-layer approach:**

### Layer 1: MD5 surrogate keys in staging

During transform, every record gets an MD5 hash computed from its business key fields:

```python
# fact_transactions hash key
transaction_hash = MD5(InvoiceNo | StockCode | InvoiceDate | Quantity | UnitPrice | CustomerID)

# audit_rejected_records hash key
reject_hash = MD5(InvoiceNo | StockCode | InvoiceDate | Quantity | UnitPrice | rejection_reason)
```

These hashes are **deterministic** — the same record always produces the same hash, regardless of when the DAG runs.

### Layer 2: PostgreSQL `ON CONFLICT` upsert

The load step uses `ON CONFLICT DO NOTHING` for facts (a re-inserted fact with the same hash is silently ignored) and `ON CONFLICT DO UPDATE` for dimensions (a re-inserted dimension updates the mutable fields but the primary key stays the same).

```sql
-- Facts: idempotent insert
INSERT INTO fact_transactions (...) VALUES %s
ON CONFLICT (transaction_hash) DO NOTHING;

-- Dimensions: idempotent upsert
INSERT INTO dim_products (...) VALUES %s
ON CONFLICT (stock_code) DO UPDATE SET
    canonical_description = EXCLUDED.canonical_description,
    ...
```

**Result:** Re-running the full DAG on the same dataset leaves the DW in an identical state — no duplicates, no data loss.

---

## 5. Performance Decisions

| Decision | Justification |
|---|---|
| **Parquet as staging format** | 95 MB CSV → ~7 MB Parquet. Columnar format with snappy compression. Read time reduced by ~10x vs CSV re-reading. |
| **`psycopg2.extras.execute_values` with `page_size=20000`** | Bulk-inserts 20,000 rows per round trip to PostgreSQL. Dramatically faster than row-by-row inserts or SQLAlchemy ORM. |
| **Vectorized category assignment with `np.select`** | Applies regex-based category logic across the entire DataFrame in one pass. Avoids Python-level row iteration (`.apply()`), which would be ~50x slower on 534K rows. |
| **`fact_daily_product_metrics` pre-aggregation** | Business questions about monthly revenue and return rates aggregate 534K fact rows. Pre-computing the daily totals during ETL means BI queries hit 278K aggregated rows instead, running in milliseconds. |
| **Parallel extraction tasks in Airflow** | `extract_daily_sales_csv` and `extract_historical_sales_csv` run in parallel (both depend on nothing; `transform` depends on both). Saves ~30% of pipeline wall-clock time. |

---

## 6. Product Category Assignment — Hybrid Strategy

The pipeline uses a **two-tier hybrid strategy** to assign categories to products:

1. **FastAPI Catalog API** (primary): If `use_catalog_api=true` (Airflow Variable), the transform task queries `GET /products?limit=10000` and maps `stock_code → category`. The API is seeded with 3,828 products pre-classified into the 5 business categories.

2. **Vectorized heuristic fallback** (secondary): Any product not found in the API (or if the API is unavailable) gets a category via regex keyword matching on its `canonical_description`:
   - `Electrónica`: LIGHT, LAMP, CLOCK, USB, CAMERA, BATTERY, LED…
   - `Papelería`: PAPER, PEN, NOTEBOOK, CARD, WRAP, TAPE…
   - `Ropa`: BAG, SHIRT, DRESS, HAT, SCARF, APRON…
   - `Deportes`: BALL, GAME, BIKE, PUZZLE, TOY, OUTDOOR…
   - `Hogar`: everything else (default — matches most products in this catalog)

**Result:** The pipeline works correctly whether or not the catalog API is running, satisfying the spec's requirement for resilience.

---

## 7. Docker & Infrastructure Decisions

| Decision | Justification |
|---|---|
| **Two separate PostgreSQL instances** | The spec explicitly requires Airflow metadata DB and the analytical DW to be separate. Keeps Airflow's internal schema completely isolated from business data. |
| **`AIRFLOW_CONN_*` and `AIRFLOW_VAR_*` env vars** | Airflow 3.x auto-creates Connections and Variables from environment variables on startup. Zero manual UI configuration — the environment is fully reproducible from `.env` alone. |
| **`uv` package manager in Dockerfile** | `uv` is significantly faster than `pip` for resolving and installing packages in Docker build layers. Reduces build time from ~3 minutes to ~45 seconds. |
| **`.dockerignore` excludes `data_sources/` and `logs/`** | CSVs (95 MB each) and logs are not needed in the Docker build context. Excluding them cuts build context transfer time and keeps images lean. |
| **Star schema DDL in `database_init/init_schema.sql`** | The `warehouse-postgres` container mounts this file into `/docker-entrypoint-initdb.d/`. PostgreSQL executes it automatically on first startup — tables exist before Airflow even boots. |

---

## 8. What Would Be Next (Roadmap)

If more time were available, the following improvements would add significant value:

1. **Data model diagram** — A dbdiagram.io or Mermaid diagram showing all 6 tables and their FK relationships, embedded in the README.
2. **dbt for transformation layer** — Replace the pandas transform script with dbt models for versioned, testable SQL transformations.
3. **Data quality tests** — Add `Great Expectations` assertions on staging Parquets before loading (e.g., `fact_transactions.unit_price > 0` on 100% of rows).
4. **Alerting** — Configure Airflow email/Slack callbacks on DAG failure.
5. **Incremental loading** — Instead of full-refresh staging, use the DAG's `logical_date` to process only the day's new records, making the pipeline truly streaming-ready.
