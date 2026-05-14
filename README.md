# End-to-End ELT Pipeline with Python, BigQuery, dbt, GitHub Actions, and Airflow

This project demonstrates an end-to-end ELT workflow using Python, BigQuery, dbt, GitHub Actions, and Apache Airflow. It includes API ingestion, raw warehouse loading, dbt transformations, tests, freshness checks, snapshots, incremental modeling, scheduled automation, local Airflow orchestration, ingestion logging, and validation.

![dbt and Python CI](https://github.com/kachiokeke/dbt-bq-test/actions/workflows/ci.yml/badge.svg?branch=dbt-concepts)

## Project Overview

This project demonstrates an end-to-end analytics engineering and beginner data engineering workflow using *Python, BigQuery, dbt, GitHub Actions, and Apache Airflow*.

The project has two connected workflows:

1. *HubSpot Sales Pipeline Analytics*  
   Raw HubSpot deal data already exists in BigQuery and is transformed with dbt into tested sales pipeline marts.

2. *Fake Store API ELT Pipeline*  
   Product data is extracted from a public API using Python, loaded into BigQuery, transformed with dbt, validated, automated with GitHub Actions, and orchestrated locally with Airflow.

The goal is to show how raw operational/API data can be ingested, cleaned, transformed, tested, validated, monitored, and prepared for reporting.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | API extraction, JSON flattening, BigQuery loading, validation scripts |
| BigQuery | Cloud data warehouse for raw and transformed data |
| dbt | SQL transformations, tests, source freshness, snapshots, incremental models |
| GitHub | Version control |
| GitHub Actions | CI checks and scheduled ELT automation |
| Apache Airflow | Local pipeline orchestration |
| Docker Compose | Local Airflow environment |
| HubSpot Data | CRM sales pipeline source data |
| Fake Store / Public Product API | Ecommerce API source data |

---

## Architecture

text
External API / Raw Source Data
        ↓
Python Ingestion
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
Tests, Freshness Checks, Snapshots, Validation
        ↓
GitHub Actions / Airflow Orchestration


---

# Workflow 1: HubSpot Sales Pipeline Analytics

## Source

Raw HubSpot deals data is stored in BigQuery:

text
pprojects-488010.pprojects_488010_marketing_data.deals


The table includes:

- Deal IDs
- Deal names
- Company associations
- Pipeline IDs
- Deal stage IDs
- Deal source
- Deal amount fields
- Created and close dates
- HubSpot owner/team fields
- Activity fields
- Attribution fields
- Airbyte metadata

---

## HubSpot dbt Flow

text
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


---

## HubSpot Models

### stg_hubspot_deals

Cleans and standardizes raw HubSpot deal data.

Key transformations:

- Renames raw HubSpot fields into cleaner column names
- Casts timestamps, dates, booleans, and numeric fields
- Extracts company association fields
- Preserves useful raw metadata
- Deduplicates records using extraction/update timestamps
- Produces one clean row per HubSpot deal

Grain:

text
One row per HubSpot deal


Tests:

- deal_id is not null
- deal_id is unique
- pipeline_id is not null
- deal_stage_id is not null

---

### int_hubspot_deals_enriched

Adds business logic to cleaned HubSpot deal data.

Key fields created:

| Field | Description |
|---|---|
| deal_status | Groups deals into Open, Closed Won, Closed Lost, or Closed Other |
| open_closed_status | Simplifies deals into Open or Closed |
| effective_amount | Best available deal amount using fallback logic |
| weighted_pipeline_amount | Probability-adjusted deal value |
| created_month | Month when the deal was created |
| close_month | Month when the deal is expected or marked to close |
| deal_age_days | Number of days since deal creation |
| created_to_close_days | Number of days between creation and close date |
| has_company | Indicates whether company name exists |
| has_company_association | Indicates whether a company association exists |

Grain:

text
One row per HubSpot deal


---

## Effective Amount vs Weighted Pipeline Amount

effective_amount answers:

text
How much is this deal worth based on the best available amount field?


It uses fallback logic:

sql
coalesce(
    amount,
    amount_in_home_currency,
    closed_amount,
    closed_amount_in_home_currency,
    0
)


weighted_pipeline_amount answers:

text
What is the expected value of this deal after applying stage probability?


It is calculated as:

text
effective_amount × deal_stage_probability


Example:

| Deal Amount | Stage Probability | Weighted Pipeline Amount |
|---:|---:|---:|
| 10,000 | 0.10 | 1,000 |
| 10,000 | 0.50 | 5,000 |
| 10,000 | 0.90 | 9,000 |

---

### mart_sales_pipeline

Creates a detailed sales pipeline mart grouped by month, pipeline, stage, source, status, and project type.

Metrics include:

- Deal count
- Open deal count
- Closed deal count
- Won deal count
- Lost deal count
- Total effective amount
- Total weighted pipeline amount
- Open pipeline amount
- Weighted open pipeline amount
- Won amount
- Lost amount
- Average deal amount
- Average deal age
- Closed won rate
- Closed lost rate

---

### mart_sales_pipeline_monthly

Creates an executive-level monthly sales pipeline mart.

Grain:

text
One row per created month


Metrics include:

- Deals created
- Open deals
- Closed deals
- Won deals
- Lost deals
- Total pipeline value
- Weighted pipeline value
- Average deal value
- Average deal age
- Win rate
- Loss rate

This model was converted to an *incremental model* and partitioned in BigQuery by created_month.

---

## HubSpot Snapshot

### snap_hubspot_deals

Tracks historical changes in HubSpot deals over time.

The snapshot captures changes in:

- Deal stage
- Deal status
- Amount
- Close date
- Owner/team
- Pipeline

Snapshot strategy:

text
timestamp


Unique key:

text
deal_id


Snapshot output table:

text
pprojects-488010.snapshots.snap_hubspot_deals


Snapshot metadata fields include:

| Field | Description |
|---|---|
| dbt_valid_from | When this version of the record became valid |
| dbt_valid_to | When this version stopped being valid |
| dbt_scd_id | dbt-generated identifier for each snapshot row |

---

## HubSpot Source Freshness

The HubSpot source uses _airbyte_extracted_at to monitor whether the raw source table is stale.

Example command:

bash
dbt source freshness --select source:hubspot_raw.deals


---

# Workflow 2: Fake Store API ELT Pipeline

## Source

The Fake Store / public product API provides ecommerce-style product data.

Primary endpoint:

text
https://fakestoreapi.com/products


Fallback endpoint:

text
https://dummyjson.com/products?limit=100


The fallback exists because the primary API may block some hosted environments such as GitHub Actions runners.

Raw BigQuery table:

text
pprojects-488010.raw_fakestore.products


---

## Fake Store ELT Flow

text
Public Product API
    ↓
Python ingestion script
    ↓
raw_fakestore.products
    ↓
fakestore_raw.products dbt source
    ↓
stg_fakestore_products
    ↓
mart_product_catalog
    ↓
Airflow validation task


---

## Python Ingestion

Script:

text
ingestion/fakestore/extract_products_to_bigquery.py


The script:

1. Calls the public product API.
2. Falls back to DummyJSON if the primary API fails.
3. Extracts product records.
4. Flattens nested rating data.
5. Adds ingestion metadata.
6. Loads the data into BigQuery.
7. Writes success/failure run metadata to an ingestion log table.

Raw fields loaded include:

| Field | Description |
|---|---|
| id | Product ID |
| title | Product title |
| price | Product price |
| description | Product description |
| category | Product category |
| image | Product image URL |
| rating_rate | Average product rating |
| rating_count | Number of product ratings / stock proxy from fallback |
| _ingested_at | Timestamp when the data was loaded |
| _source | Source system label |

Current load strategy:

text
WRITE_TRUNCATE


This means the raw products table is replaced on each run.

---

## Ingestion Run Logging

The ingestion script writes operational metadata to:

text
pprojects-488010.raw_fakestore.ingestion_run_log


The log table captures:

| Field | Description |
|---|---|
| run_id | Unique run identifier |
| source_name | API/source used |
| target_table | BigQuery table loaded |
| rows_loaded | Number of rows loaded |
| started_at | Run start timestamp |
| completed_at | Run completion timestamp |
| status | Success or failed |
| error_message | Error details if the run failed |

This provides basic pipeline observability and auditability.

---

## Fake Store Source Freshness

The Fake Store source uses _ingested_at as its freshness timestamp.

Example command:

bash
dbt source freshness --select source:fakestore_raw.products


---

## Fake Store dbt Models

### stg_fakestore_products

Cleans and standardizes raw product data.

Key transformations:

- Renames raw fields into business-friendly names
- Casts IDs, prices, ratings, and timestamps
- Standardizes product category formatting
- Deduplicates product records
- Produces one clean row per product

Grain:

text
One row per product


Tests:

- product_id is not null
- product_id is unique
- product_title is not null
- product_price is not null
- product_category is not null
- ingested_at is not null

---

### mart_product_catalog

Aggregates product data by category.

Grain:

text
One row per product category


Metrics include:

| Metric | Description |
|---|---|
| product_count | Number of products in the category |
| min_product_price | Lowest product price in the category |
| max_product_price | Highest product price in the category |
| average_product_price | Average product price in the category |
| total_catalog_value | Sum of product prices in the category |
| average_rating | Average product rating |
| total_rating_count | Total number of ratings |
| highly_rated_product_count | Number of products with rating of 4 or higher |
| premium_product_count | Number of products priced at 100 or above |

Tests:

- product_category is not null
- product_category is unique
- product_count is not null

---

## Airflow Validation

A validation script checks that product totals reconcile between staging and mart outputs.

Script:

text
ingestion/fakestore/validate_product_catalog.py


Validation checks:

- Product count in staging equals product count in mart
- Product value in staging equals catalog value in mart

If the values do not match, the Airflow validation task fails.

---

# Automation and Orchestration

## GitHub Actions CI

Workflow file:

text
.github/workflows/ci.yml


The CI workflow runs on push and pull request events.

It validates:

- Python dependency installation
- Python ingestion script syntax
- dbt dependency installation
- temporary dbt profile creation
- dbt parse
- dbt compile with BigQuery credentials

Current CI flow:

text
Push / Pull Request
    ↓
Checkout repository
    ↓
Set up Python
    ↓
Install Python dependencies
    ↓
Check Python syntax
    ↓
Install dbt
    ↓
Create dbt CI profile
    ↓
Run dbt parse
    ↓
Run dbt compile
    ↓
Pass / Fail


---

## Scheduled GitHub Actions Workflow

Workflow file:

text
.github/workflows/scheduled_pipeline.yml


This workflow runs the Fake Store ELT pipeline on a schedule or manual trigger.

It performs:

- Python dependency setup
- GCP service account setup from GitHub Secrets
- API ingestion into BigQuery
- dbt source freshness check
- dbt build for mart_product_catalog

Current trigger:

text
schedule + workflow_dispatch


The temporary push trigger used during development has been removed.

---

## Airflow Orchestration

Airflow runs locally using Docker Compose.

Airflow DAG file:

text
airflow/dags/fakestore_elt_dag.py


DAG:

text
fakestore_elt_pipeline


Task flow:

text
start_pipeline
    ↓
run_python_ingestion
    ↓
check_fakestore_source_freshness
    ↓
build_product_catalog_mart
    ↓
validate_product_catalog
    ↓
finish_pipeline


The DAG uses:

- BashOperator to run Python and dbt commands
- Retry configuration
- Local Docker-mounted GCP service account credentials
- .env.airflow for container-specific runtime configuration

---

# dbt Project Structure

text
models/
  staging/
    hubspot/
      _hubspot_sources.yml
      stg_hubspot_deals.sql
      stg_hubspot_deals.yml

    fakestore/
      _fakestore_sources.yml
      stg_fakestore_products.sql
      stg_fakestore_products.yml

  intermediate/
    hubspot/
      int_hubspot_deals_enriched.sql
      int_hubspot_deals_enriched.yml

  marts/
    sales/
      mart_sales_pipeline.sql
      mart_sales_pipeline.yml
      mart_sales_pipeline_monthly.sql
      mart_sales_pipeline_monthly.yml

    ecommerce/
      mart_product_catalog.sql
      mart_product_catalog.yml

snapshots/
  snap_hubspot_deals.sql
  snap_hubspot_deals.yml

ingestion/
  fakestore/
    extract_products_to_bigquery.py
    validate_product_catalog.py
    requirements.txt
    .env.example

airflow/
  dags/
    fakestore_elt_dag.py
  dbt_profiles/
    profiles.yml
  Dockerfile
  docker-compose.yaml
  requirements.txt

.github/
  workflows/
    ci.yml
    scheduled_pipeline.yml


---

## dbt Ignore Configuration

Because this repository includes dbt, Python, GitHub Actions, Airflow, and Docker files, .dbtignore is used to prevent dbt from parsing non-dbt project files.

Ignored folders include:

text
.github/
airflow/
ingestion/
logs/
target/
dbt_packages/
dbt_internal_packages/


This prevents dbt parse/compile errors from non-dbt YAML or configuration files.

---

## Materialization Strategy

| Layer | Materialization | Reason |
|---|---|---|
| Staging | View | Lightweight cleaning and easy inspection |
| Intermediate | View | Reusable business logic |
| Marts | Table / Incremental | Reporting-ready outputs |
| Snapshot | Snapshot table | Historical record tracking |

The HubSpot monthly mart uses:

- Incremental materialization
- BigQuery merge strategy
- Partitioning by created_month

---

## Validation Checks

### Fake Store Raw Validation

sql
select
    count(*) as total_rows,
    count(distinct id) as distinct_products,
    countif(id is null) as missing_id,
    countif(title is null) as missing_title,
    countif(price is null) as missing_price,
    countif(category is null) as missing_category,
    max(_ingested_at) as latest_ingested_at
from `pprojects-488010.raw_fakestore.products`;


---

### Fake Store Mart Validation

sql
select
    'staging' as layer,
    count(*) as total_products,
    round(sum(product_price), 2) as total_catalog_value
from `pprojects-488010.dbt_kachiokeke.stg_fakestore_products`

union all

select
    'mart' as layer,
    sum(product_count) as total_products,
    round(sum(total_catalog_value), 2) as total_catalog_value
from `pprojects-488010.dbt_kachiokeke.mart_product_catalog`;


Expected:

text
staging totals = mart totals


---

### Ingestion Log Validation

sql
select
    run_id,
    source_name,
    target_table,
    rows_loaded,
    started_at,
    completed_at,
    timestamp_diff(completed_at, started_at, second) as duration_seconds,
    status,
    error_message
from `pprojects-488010.raw_fakestore.ingestion_run_log`
order by completed_at desc
limit 10;


Expected:

text
status = success
error_message = null


---

### HubSpot Monthly Mart Validation

sql
select
    'intermediate' as layer,
    count(*) as total_deals,
    sum(effective_amount) as total_pipeline_value,
    sum(weighted_pipeline_amount) as weighted_pipeline_value
from `pprojects-488010.dbt_kachiokeke.int_hubspot_deals_enriched`

union all

select
    'monthly_mart' as layer,
    sum(deals_created) as total_deals,
    sum(total_pipeline_value) as total_pipeline_value,
    sum(weighted_pipeline_value) as weighted_pipeline_value
from `pprojects-488010.dbt_kachiokeke.mart_sales_pipeline_monthly`;


Expected:

text
intermediate totals = monthly mart totals


---

## Key Commands

Run Fake Store ingestion locally:

bash
cd ingestion/fakestore
source .venv/bin/activate
ENV_FILE=.env.local python extract_products_to_bigquery.py


Build Fake Store mart:

bash
dbt build --select +mart_product_catalog


Run Fake Store source freshness:

bash
dbt source freshness --select source:fakestore_raw.products


Build HubSpot monthly mart:

bash
dbt build --select +mart_sales_pipeline_monthly


Run HubSpot snapshot:

bash
dbt snapshot --select snap_hubspot_deals


Start Airflow locally:

bash
cd airflow
docker compose up -d


Trigger Airflow DAG:

text
Airflow UI → DAGs → fakestore_elt_pipeline → Trigger


---

## Business Questions Answered

### HubSpot Sales Pipeline

- How many deals were created each month?
- How much total pipeline value exists?
- What is the probability-weighted pipeline value?
- How many deals are open, won, or lost?
- What is the monthly win rate?
- What is the average deal value?
- What is the average deal age?
- How has deal data changed over time?

### Fake Store Product Catalog

- How many products exist per category?
- What is the average product price by category?
- Which category has the highest catalog value?
- Which categories have highly rated products?
- Which categories contain premium products?
- How fresh is the ingested API data?
- Did the mart reconcile with staging after transformation?

---

## Skills Demonstrated

- Python API extraction
- JSON flattening
- BigQuery loading with Python
- Environment variable management
- Service account authentication
- BigQuery raw table design
- Ingestion run logging
- dbt source configuration
- dbt staging models
- dbt intermediate models
- dbt marts
- dbt data tests
- dbt source freshness
- dbt snapshots
- Incremental dbt models
- BigQuery partitioning
- BigQuery validation queries
- GitHub Actions CI
- Scheduled GitHub Actions workflows
- Airflow DAG orchestration
- Docker Compose
- Git/GitHub version control

---

## Known Limitations

- Fake Store ingestion currently uses full refresh loading with WRITE_TRUNCATE.
- API row counts may differ by environment if the primary API fails and the fallback API is used.
- Fake Store API data is small and used for learning/demo purposes.
- HubSpot stage IDs have not yet been mapped to readable stage names.
- HubSpot owner/team IDs have not yet been joined to dimension tables.
- HubSpot deal-company and deal-contact associations are not yet modeled as separate bridge tables.
- Airflow currently runs locally, not in a cloud-managed environment.
- No dashboard layer has been added yet.
- Alerting/notifications have not yet been implemented.

---

## Recommended Next Improvements

1. Add final architecture diagrams.
2. Add a Looker Studio dashboard connected to the dbt marts.
3. Convert Fake Store ingestion from full refresh to append/merge logic.
4. Add additional Fake Store endpoints such as carts and users.
5. Add HubSpot stage mapping using a dbt seed.
6. Add bridge models for HubSpot deal-company and deal-contact associations.
7. Add dbt exposures for dashboards.
8. Add alerting for failed Airflow or GitHub Actions runs.
9. Move Airflow orchestration to a cloud environment such as Cloud Composer.
10. Add more robust data quality checks.

---

## Project Summary

This project demonstrates a complete modern ELT workflow:

text
API / Raw Source
    ↓
Python Ingestion
    ↓
BigQuery Raw Layer
    ↓
dbt Transformations
    ↓
Tests, Freshness, Snapshots, Validation
    ↓
GitHub Actions Automation
    ↓
Airflow Orchestration


The result is a maintainable analytics engineering and data engineering project that turns raw CRM and API data into tested, validated, and reporting-ready datasets.