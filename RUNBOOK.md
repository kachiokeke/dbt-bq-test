# Pipeline Runbook

## Project

This runbook explains how to run, validate, and troubleshoot the ELT pipeline built with Python, BigQuery, dbt, GitHub Actions, and Apache Airflow.

The project includes two main workflows:

1. **HubSpot Sales Pipeline Analytics**
2. **Fake Store / Public Product API ELT Pipeline**

---

## High-Level Architecture

```text
API / Raw Source
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
Tests, Freshness, Snapshots, Validation
    ↓
GitHub Actions / Airflow
```

---

# 1. Environment and Secrets

## Required Local Files

The project uses environment files for local and Airflow-specific runs.

Local ingestion config:

```text
ingestion/fakestore/.env.local
```

Airflow ingestion config:

```text
ingestion/fakestore/.env.airflow
```

Example local config:

```env
GCP_PROJECT_ID=pprojects-488010
BQ_DATASET_ID=raw_fakestore
BQ_TABLE_ID=products
DBT_DATASET_ID=dbt_kachiokeke
GOOGLE_APPLICATION_CREDENTIALS=/Users/kachi_1/Downloads/pprojects-488010-bbec760c354d.json
```

Example Airflow config:

```env
GCP_PROJECT_ID=pprojects-488010
BQ_DATASET_ID=raw_fakestore
BQ_TABLE_ID=products
DBT_DATASET_ID=dbt_kachiokeke
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-service-account.json
```

Important:

```text
.env files and service account JSON files must not be committed to GitHub.
```

---

## GitHub Secrets

The GitHub Actions workflows require this repository secret:

```text
GCP_SA_KEY
```

This secret should contain the full service account JSON contents, not the file path.

---

## Airflow Docker Credential Mount

In `airflow/docker-compose.yaml`, the service account file is mounted into the Airflow container.

Example:

```yaml
- /Users/kachi_1/Downloads/pprojects-488010-bbec760c354d.json:/opt/airflow/gcp-service-account.json:ro
```

Meaning:

```text
Mac path:
/Users/kachi_1/Downloads/pprojects-488010-bbec760c354d.json

Container path:
/opt/airflow/gcp-service-account.json
```

Inside Airflow/Docker, always use the container path.

---

# 2. Fake Store API ELT Pipeline

## Flow

```text
Public Product API
    ↓
Python ingestion script
    ↓
BigQuery raw_fakestore.products
    ↓
dbt source freshness
    ↓
stg_fakestore_products
    ↓
mart_product_catalog
    ↓
Airflow validation
```

---

## 2.1 Run Python Ingestion Locally

From the project root:

```bash
cd ingestion/fakestore
source .venv/bin/activate
ENV_FILE=.env.local python extract_products_to_bigquery.py
```

Expected output:

```text
Extracted product records from API.
Loaded rows into pprojects-488010.raw_fakestore.products
```

The script also writes an ingestion run record to:

```text
pprojects-488010.raw_fakestore.ingestion_run_log
```

---

## 2.2 Validate Raw Product Data

Run this in BigQuery:

```sql
select
    count(*) as total_rows,
    count(distinct id) as distinct_products,
    countif(id is null) as missing_id,
    countif(title is null) as missing_title,
    countif(price is null) as missing_price,
    countif(category is null) as missing_category,
    max(_ingested_at) as latest_ingested_at
from `pprojects-488010.raw_fakestore.products`;
```

Expected:

```text
total_rows = distinct_products
missing_id = 0
```

---

## 2.3 Validate Ingestion Run Log

Run this in BigQuery:

```sql
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
```

Expected:

```text
status = success
error_message = null
```

---

## 2.4 Check Fake Store Source Freshness

Run:

```bash
dbt source freshness --select source:fakestore_raw.products
```

This checks freshness using:

```text
_ingested_at
```

---

## 2.5 Build Fake Store dbt Models

Run:

```bash
dbt build --select +mart_product_catalog
```

This builds:

```text
stg_fakestore_products
    ↓
mart_product_catalog
```

---

## 2.6 Validate Fake Store Mart

Run this in BigQuery:

```sql
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
```

Expected:

```text
staging total_products = mart total_products
staging total_catalog_value = mart total_catalog_value
```

---

# 3. HubSpot Sales Pipeline Workflow

## Flow

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

---

## 3.1 Check HubSpot Source Freshness

Run:

```bash
dbt source freshness --select source:hubspot_raw.deals
```

This checks freshness using:

```text
_airbyte_extracted_at
```

---

## 3.2 Build HubSpot Monthly Mart

Run:

```bash
dbt build --select +mart_sales_pipeline_monthly
```

This builds:

```text
stg_hubspot_deals
    ↓
int_hubspot_deals_enriched
    ↓
mart_sales_pipeline_monthly
```

---

## 3.3 Build HubSpot Detailed Mart

Run:

```bash
dbt build --select +mart_sales_pipeline
```

This builds the detailed sales pipeline mart.

---

## 3.4 Run HubSpot Snapshot

Snapshots are run separately from regular dbt model builds.

Run:

```bash
dbt snapshot --select snap_hubspot_deals
```

The snapshot tracks historical changes in HubSpot deals.

---

## 3.5 Validate HubSpot Monthly Mart

Run this in BigQuery:

```sql
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
```

Expected:

```text
intermediate totals = monthly mart totals
```

---

## 3.6 Validate HubSpot Snapshot

Run:

```sql
select
    count(*) as total_snapshot_rows,
    count(distinct deal_id) as distinct_deals,
    countif(dbt_valid_to is null) as current_rows,
    countif(dbt_valid_to is not null) as historical_rows
from `pprojects-488010.snapshots.snap_hubspot_deals`;
```

Expected:

```text
current_rows = distinct_deals
historical_rows may be 0 until records change
```

---

# 4. GitHub Actions

The project has two GitHub Actions workflows:

```text
.github/workflows/ci.yml
.github/workflows/scheduled_pipeline.yml
```

---

## 4.1 CI Workflow

Workflow:

```text
dbt and Python CI
```

File:

```text
.github/workflows/ci.yml
```

This workflow runs on push and pull request events.

It validates:

- Python dependency installation
- Python script syntax
- dbt dependency installation
- dbt profile creation
- `dbt parse`
- `dbt compile`

To check workflow status:

```text
GitHub → Repository → Actions → dbt and Python CI
```

Expected:

```text
Success
```

---

## 4.2 Scheduled Fake Store ELT Workflow

Workflow:

```text
Scheduled Fake Store ELT Pipeline
```

File:

```text
.github/workflows/scheduled_pipeline.yml
```

This workflow runs on:

```text
schedule
workflow_dispatch
```

The temporary push trigger used during development has been removed.

The workflow performs:

- Python setup
- GCP service account setup from GitHub Secrets
- Product API ingestion
- BigQuery raw table loading
- dbt source freshness
- dbt build for `mart_product_catalog`

To run manually:

```text
GitHub → Actions → Scheduled Fake Store ELT Pipeline → Run workflow
```

Expected:

```text
Success
```

---

# 5. Airflow Local Orchestration

Airflow runs locally through Docker Compose.

Airflow folder:

```text
airflow/
```

DAG file:

```text
airflow/dags/fakestore_elt_dag.py
```

DAG name:

```text
fakestore_elt_pipeline
```

---

## 5.1 Start Airflow

From the project root:

```bash
cd airflow
docker compose up -d
```

Check services:

```bash
docker compose ps
```

Expected services include:

```text
airflow-apiserver
airflow-scheduler
airflow-worker
airflow-dag-processor
airflow-triggerer
postgres
redis
```

---

## 5.2 Open Airflow UI

Open:

```text
http://localhost:8080
```

Login:

```text
Username: airflow
Password: airflow
```

---

## 5.3 Trigger Airflow DAG

In the Airflow UI:

```text
DAGs → fakestore_elt_pipeline → Trigger
```

Expected task flow:

```text
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
```

Expected:

```text
All tasks green
DAG run success
```

---

## 5.4 Stop Airflow

From the `airflow/` folder:

```bash
docker compose down
```

---

## 5.5 Restart Airflow

```bash
docker compose up -d
```

---

## 5.6 View Airflow Logs

From the `airflow/` folder:

```bash
docker compose logs -f
```

For a specific service:

```bash
docker compose logs airflow-scheduler --tail=100
```

---

# 6. Airflow Validation Task

The validation task runs:

```text
ingestion/fakestore/validate_product_catalog.py
```

It checks:

- Product count in staging equals product count in mart
- Catalog value in staging equals catalog value in mart

If validation fails, the Airflow DAG fails.

To run validation locally:

```bash
cd ingestion/fakestore
source .venv/bin/activate
ENV_FILE=.env.local python validate_product_catalog.py
```

Expected:

```text
Validation passed: staging and mart totals match.
```

---

# 7. Common Issues and Fixes

## Issue: Airflow DAG does not show up

Possible causes:

- DAG file not saved
- DAG file not in `airflow/dags/`
- Airflow has not refreshed
- Python import error in DAG

Fix:

```bash
cd airflow
docker compose down
docker compose up -d
```

Check logs:

```bash
docker compose logs airflow-dag-processor --tail=100
```

---

## Issue: Airflow uses wrong credential path

Symptom:

```text
File /Users/.../service-account.json was not found
```

Cause:

Airflow is using the Mac path inside Docker.

Fix:

For Airflow, use the container path:

```env
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-service-account.json
```

Make sure Docker Compose mounts the Mac file into the container:

```yaml
- /Users/kachi_1/Downloads/pprojects-488010-bbec760c354d.json:/opt/airflow/gcp-service-account.json:ro
```

---

## Issue: Local script uses wrong credential path

Symptom:

```text
File /opt/airflow/gcp-service-account.json was not found
```

Cause:

You are running locally but using the Airflow container path.

Fix:

For local runs, use the Mac path:

```env
GOOGLE_APPLICATION_CREDENTIALS=/Users/kachi_1/Downloads/pprojects-488010-bbec760c354d.json
```

Run:

```bash
ENV_FILE=.env.local python extract_products_to_bigquery.py
```

---

## Issue: dbt parses non-dbt YAML files

Symptom:

```text
KeyError: javascript
```

or parse errors from Airflow/GitHub YAML files.

Cause:

dbt is trying to parse non-dbt files.

Fix:

Use `.dbtignore` to ignore non-dbt folders:

```text
.github/
airflow/
ingestion/
logs/
target/
dbt_packages/
dbt_internal_packages/
```

---

## Issue: Python type hint error

Symptom:

```text
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
```

Cause:

Local Python version is below 3.10.

Fix:

Use:

```python
from typing import Optional
```

Instead of:

```python
str | None
```

Use:

```python
Optional[str]
```

---

## Issue: Fake Store API returns 403

Cause:

The primary API may block cloud/hosted environments.

Fix:

The ingestion script includes a fallback API:

```text
https://dummyjson.com/products?limit=100
```

No action required unless both APIs fail.

---

## Issue: GitHub Actions badge does not render

Use the branch-specific badge:

```markdown
![dbt and Python CI](https://github.com/kachiokeke/dbt-bq-test/actions/workflows/ci.yml/badge.svg?branch=dbt-concepts)
```

---

# 8. Git and Commit Checklist

Before committing, run:

```bash
git status
```

Safe files to commit:

```text
.py
.sql
.yml
.md
Dockerfile
docker-compose.yaml
requirements.txt
.dbtignore
.gitignore
```

Do not commit:

```text
.env
.env.local
.env.airflow
*.json
.DS_Store
__pycache__/
airflow/logs/
airflow/config/
target/
dbt_packages/
dbt_internal_packages/
```

Recommended `.gitignore` entries:

```gitignore
# macOS
.DS_Store
**/.DS_Store

# Python
__pycache__/
**/__pycache__/
*.pyc
.venv/
venv/

# Secrets
.env
**/.env
.env.local
.env.airflow
*.json

# dbt
target/
dbt_packages/
dbt_internal_packages/
logs/

# Airflow
airflow/logs/
airflow/.env
airflow/config/
```

---

# 9. Full Manual Run Order

## Fake Store Pipeline

```bash
cd ingestion/fakestore
source .venv/bin/activate
ENV_FILE=.env.local python extract_products_to_bigquery.py
```

Then:

```bash
dbt source freshness --select source:fakestore_raw.products
dbt build --select +mart_product_catalog
```

Validate:

```bash
ENV_FILE=.env.local python validate_product_catalog.py
```

---

## HubSpot Pipeline

```bash
dbt source freshness --select source:hubspot_raw.deals
dbt build --select +mart_sales_pipeline_monthly
dbt build --select +mart_sales_pipeline
dbt snapshot --select snap_hubspot_deals
```

---

## Airflow Pipeline

```bash
cd airflow
docker compose up -d
```

Then:

```text
Airflow UI → Trigger fakestore_elt_pipeline
```

---

# 10. Final Project Status

Completed stages:

- Python API ingestion
- BigQuery raw loading
- Ingestion run logging
- dbt source definitions
- dbt staging models
- dbt intermediate models
- dbt marts
- dbt tests
- dbt source freshness
- dbt snapshots
- Incremental dbt model
- BigQuery partitioning
- GitHub Actions CI
- Scheduled GitHub Actions pipeline
- Airflow local orchestration
- Airflow validation task

---

# 11. Future Improvements

Recommended next improvements:

1. Add architecture diagrams.
2. Add Looker Studio dashboard.
3. Convert API ingestion from full refresh to append/merge.
4. Add more API endpoints such as carts and users.
5. Add HubSpot stage mapping with a dbt seed.
6. Add bridge models for HubSpot associations.
7. Add dbt exposures for dashboards.
8. Add alerting for Airflow/GitHub Actions failures.
9. Deploy Airflow to a cloud-managed environment.
10. Add more robust data quality checks.