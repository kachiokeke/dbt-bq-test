![dbt and Python CI](https://github.com/kachiokeke/dbt-bq-test/actions/workflows/ci.yml/badge.svg)


# dbt + BigQuery ELT Project

## Overview

This project demonstrates an analytics engineering and beginner data engineering workflow using **Python, BigQuery, dbt, and GitHub**.

It contains two workflows:

1. **HubSpot Sales Pipeline Analytics**  
   Raw HubSpot deal data already exists in BigQuery and is transformed with dbt.

2. **Fake Store API ELT Pipeline**  
   Product data is extracted from a public API using Python, loaded into BigQuery, then transformed with dbt.

The goal is to show how raw data can be ingested, cleaned, modeled, tested, validated, and prepared for reporting.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | API extraction and BigQuery loading |
| BigQuery | Cloud data warehouse |
| dbt | SQL transformation, testing, snapshots, freshness checks |
| GitHub | Version control |
| Fake Store API | Public ecommerce API source |
| HubSpot Data | CRM sales pipeline source |

---

## Architecture

```text
External / Raw Data
    ↓
Python or Existing BigQuery Raw Tables
    ↓
BigQuery Raw Layer
    ↓
dbt Sources
    ↓
dbt Staging Models
    ↓
dbt Intermediate Models
    ↓
dbt Marts
    ↓
Validation / Reporting
```

---

# Workflow 1: HubSpot Sales Pipeline Analytics

## Source

Raw HubSpot deals table:

```text
pprojects-488010.pprojects_488010_marketing_data.deals
```

## dbt Flow

```text
hubspot_raw.deals
    ↓
stg_hubspot_deals
    ↓
int_hubspot_deals_enriched
    ↓
mart_sales_pipeline
mart_sales_pipeline_monthly
    ↓
snap_hubspot_deals
```

## Models

### `stg_hubspot_deals`

Cleans and standardizes raw HubSpot deal data.

Key work:

- Renames raw fields
- Casts dates, timestamps, booleans, and numeric fields
- Extracts company association fields
- Deduplicates records
- Produces one row per deal

Grain:

```text
One row per HubSpot deal
```

Tests:

- `deal_id` is not null
- `deal_id` is unique
- `pipeline_id` is not null
- `deal_stage_id` is not null

---

### `int_hubspot_deals_enriched`

Adds business logic to the cleaned deal data.

Key fields:

| Field | Description |
|---|---|
| `deal_status` | Open, Closed Won, Closed Lost, or Closed Other |
| `open_closed_status` | Open or Closed |
| `effective_amount` | Best available deal amount |
| `weighted_pipeline_amount` | Probability-adjusted deal value |
| `created_month` | Month when deal was created |
| `close_month` | Month when deal is expected/marked to close |
| `deal_age_days` | Days since deal creation |
| `has_company` | Whether company name exists |
| `has_company_association` | Whether company association exists |

---

### `mart_sales_pipeline`

Detailed sales pipeline mart grouped by month, stage, source, status, and project type.

Metrics include:

- Deal count
- Open deals
- Won deals
- Lost deals
- Total pipeline value
- Weighted pipeline value
- Average deal amount
- Win rate
- Loss rate

---

### `mart_sales_pipeline_monthly`

Executive-level monthly mart.

Grain:

```text
One row per created month
```

Metrics include:

- Deals created
- Open deals
- Closed deals
- Won deals
- Lost deals
- Total pipeline value
- Weighted pipeline value
- Average deal value
- Win rate
- Loss rate

This model uses incremental materialization and BigQuery partitioning.

---

### `snap_hubspot_deals`

Tracks historical deal changes over time.

Used to monitor changes in:

- Deal stage
- Deal status
- Amount
- Close date
- Owner/team
- Pipeline

Snapshot strategy:

```text
timestamp
```

Unique key:

```text
deal_id
```

---

# Workflow 2: Fake Store API ELT Pipeline

## Source

Fake Store API endpoint:

```text
https://fakestoreapi.com/products
```

Raw BigQuery table:

```text
pprojects-488010.raw_fakestore.products
```

## Flow

```text
Fake Store API
    ↓
Python ingestion script
    ↓
raw_fakestore.products
    ↓
stg_fakestore_products
    ↓
mart_product_catalog
```

---

## Python Ingestion

Script:

```text
ingestion/fakestore/extract_products_to_bigquery.py
```

The script:

1. Calls the Fake Store API.
2. Extracts product records.
3. Flattens the nested rating object.
4. Adds ingestion metadata.
5. Loads the data into BigQuery.

Fields loaded:

| Field | Description |
|---|---|
| `id` | Product ID |
| `title` | Product title |
| `price` | Product price |
| `description` | Product description |
| `category` | Product category |
| `image` | Product image URL |
| `rating_rate` | Average product rating |
| `rating_count` | Number of ratings |
| `_ingested_at` | Ingestion timestamp |
| `_source` | Source name |

The current load strategy is:

```text
WRITE_TRUNCATE
```

This means the raw table is replaced on each run.

---

## Fake Store dbt Models

### `stg_fakestore_products`

Cleans and standardizes raw product data.

Key work:

- Renames fields
- Casts IDs, prices, ratings, and timestamps
- Standardizes category names
- Deduplicates products
- Produces one row per product

Tests:

- `product_id` is not null
- `product_id` is unique
- `product_title` is not null
- `product_price` is not null
- `product_category` is not null
- `ingested_at` is not null

---

### `mart_product_catalog`

Aggregates product data by category.

Grain:

```text
One row per product category
```

Metrics include:

| Metric | Description |
|---|---|
| `product_count` | Number of products |
| `min_product_price` | Lowest product price |
| `max_product_price` | Highest product price |
| `average_product_price` | Average product price |
| `total_catalog_value` | Sum of product prices |
| `average_rating` | Average rating |
| `total_rating_count` | Total number of ratings |
| `highly_rated_product_count` | Products rated 4 or higher |
| `premium_product_count` | Products priced at 100 or above |

---

## Project Structure

```text
models/
  staging/
    hubspot/
    fakestore/

  intermediate/
    hubspot/

  marts/
    sales/
    ecommerce/

snapshots/
  snap_hubspot_deals.sql
  snap_hubspot_deals.yml

ingestion/
  fakestore/
    extract_products_to_bigquery.py
    requirements.txt
    .env.example
```

---

## Materialization Strategy

| Layer | Materialization | Reason |
|---|---|---|
| Staging | View | Lightweight cleaning |
| Intermediate | View | Reusable business logic |
| Marts | Table / Incremental | Reporting-ready outputs |
| Snapshots | Snapshot table | Historical tracking |

---

```markdown
## GitHub Actions CI

The project includes a CI workflow that runs automatically when changes are pushed to GitHub.

To view workflow results:

1. Open the GitHub repository.
2. Go to the Actions tab.
3. Open the latest `dbt and Python CI` run.
4. Confirm the workflow completed successfully.

The current CI workflow validates Python syntax and dbt project parsing. It does not yet run the full BigQuery pipeline.

## Key Commands

Build Fake Store pipeline:

```bash
dbt build --select +mart_product_catalog
```

Build HubSpot monthly mart:

```bash
dbt build --select +mart_sales_pipeline_monthly
```

Build HubSpot detailed mart:

```bash
dbt build --select +mart_sales_pipeline
```

Run HubSpot snapshot:

```bash
dbt snapshot --select snap_hubspot_deals
```

Check Fake Store source freshness:

```bash
dbt source freshness --select source:fakestore_raw.products
```

Check HubSpot source freshness:

```bash
dbt source freshness --select source:hubspot_raw.deals
```

The project includes a CI workflow that runs automatically when changes are pushed to GitHub.

To check the workflow:

1. Open the GitHub repository.
2. Go to the **Actions** tab.
3. Open the latest `dbt and Python CI` run.
4. Confirm that `Run dbt and Python checks` completed successfully.

The workflow currently validates Python syntax and dbt project compilation. It does not yet run the full ELT pipeline or execute dbt models in BigQuery.

Current CI checks:

```text
Python dependency install
Python syntax check
dbt dependency install
dbt profile creation
dbt parse
dbt compile
```

---

## Validation

Validation queries were used to confirm that:

- Raw row counts matched staged row counts.
- Staging totals matched mart totals.
- Primary keys were unique.
- Required fields were not null.
- Snapshot current rows matched distinct deals.
- Source freshness checks passed.

---

## Skills Demonstrated

- Python API extraction
- JSON flattening
- BigQuery loading with Python
- Environment variable management
- Service account authentication
- dbt source configuration
- dbt staging models
- dbt intermediate models
- dbt marts
- dbt data tests
- dbt snapshots
- dbt source freshness
- Incremental models
- BigQuery partitioning
- BigQuery validation
- Git/GitHub version control

---

## Known Limitations

- Fake Store ingestion currently uses full refresh loading.
- Fake Store API data is small and used for demonstration.
- HubSpot stage IDs have not been mapped to readable names.
- HubSpot owner/team IDs have not been joined to dimension tables.
- Deal-company and deal-contact associations are not yet modeled as bridge tables.
- The project does not yet include orchestration.
- No dashboard layer has been added yet.

---

## Recommended Next Improvements

1. Add orchestration with GitHub Actions, Airflow, or Prefect.
2. Convert Fake Store ingestion from full refresh to append/merge.
3. Add ingestion logging to BigQuery.
4. Add a Looker Studio dashboard.
5. Add HubSpot stage mapping using a dbt seed.
6. Add bridge models for deal-company and deal-contact associations.
7. Add dbt exposures for dashboards.
8. Add CI checks for dbt builds.
9. Add more API endpoints such as carts and users.

---

## Summary

This project demonstrates a complete analytics engineering and beginner data engineering workflow:

```text
API / Raw Source
    ↓
BigQuery Raw Layer
    ↓
dbt Sources
    ↓
Staging
    ↓
Intermediate Logic
    ↓
Marts
    ↓
Tests, Freshness, Snapshots, Validation
```

The result is a maintainable ELT pipeline that turns raw CRM and API data into clean, tested, reporting-ready datasets.