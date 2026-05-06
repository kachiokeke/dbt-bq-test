````markdown
# HubSpot Sales Pipeline Analytics with dbt and BigQuery

## Project Overview

This project transforms raw HubSpot deal data stored in BigQuery into clean, tested, and dashboard-ready sales pipeline marts using dbt.

The goal is to demonstrate a practical analytics engineering workflow: raw data is cleaned in a staging layer, enriched with business logic in an intermediate layer, and aggregated into reporting-ready marts for sales pipeline analysis.

## Tech Stack

| Tool | Purpose |
|---|---|
| BigQuery | Cloud data warehouse |
| dbt | Data transformation, testing, and documentation |
| GitHub | Version control |
| HubSpot Data | Source CRM/deal data |

## Project Architecture

```text
Raw HubSpot Deals Table
        ↓
dbt Source Definition
        ↓
Staging Model
        ↓
Intermediate Model
        ↓
Sales Pipeline Marts
````

Detailed flow:

```text
pprojects-488010.pprojects_488010_marketing_data.deals
        ↓
source: hubspot_raw.deals
        ↓
stg_hubspot_deals
        ↓
int_hubspot_deals_enriched
        ↓
mart_sales_pipeline
mart_sales_pipeline_monthly
```

## Data Source

The raw source table used in this project is:

```text
pprojects-488010.pprojects_488010_marketing_data.deals
```

This table contains raw HubSpot deal records, including:

* Deal IDs
* Deal names
* Company associations
* Pipeline IDs
* Deal stage IDs
* Deal source
* Deal amount fields
* Close dates
* Created and updated timestamps
* HubSpot owner/team fields
* Deal status flags
* Activity fields
* Attribution fields
* Airbyte metadata

## dbt Project Structure

```text
models/
  staging/
    hubspot/
      _hubspot_sources.yml
      stg_hubspot_deals.sql
      stg_hubspot_deals.yml

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
```

## Model Layers

### 1. Source Layer

The source layer defines the raw BigQuery HubSpot table inside dbt.

Model file:

```text
models/staging/hubspot/_hubspot_sources.yml
```

Purpose:

* Registers the raw HubSpot deals table as a dbt source.
* Allows downstream models to reference the raw table using `source()`.
* Improves lineage tracking and project maintainability.

Example reference:

```sql
{{ source('hubspot_raw', 'deals') }}
```

---

### 2. Staging Layer

Model:

```text
stg_hubspot_deals
```

Purpose:

The staging model cleans and standardizes the raw HubSpot deal data.

Key transformations include:

* Renaming raw HubSpot fields into clean column names.
* Casting timestamps, dates, booleans, and numeric values.
* Extracting company association fields.
* Preserving useful raw JSON fields.
* Deduplicating records using the latest Airbyte extraction and update timestamps.
* Creating one clean row per HubSpot deal.

Grain:

```text
One row per HubSpot deal
```

Key fields created:

| Field                  | Description                                     |
| ---------------------- | ----------------------------------------------- |
| `deal_id`              | Unique HubSpot deal ID                          |
| `deal_name`            | HubSpot deal name                               |
| `company_name`         | Associated company name                         |
| `pipeline_id`          | HubSpot pipeline ID                             |
| `deal_stage_id`        | HubSpot deal stage ID                           |
| `amount`               | Deal amount                                     |
| `close_at`             | Deal close timestamp                            |
| `is_closed`            | Whether the deal is closed                      |
| `is_closed_won`        | Whether the deal is closed won                  |
| `is_closed_lost`       | Whether the deal is closed lost                 |
| `airbyte_extracted_at` | Timestamp when Airbyte extracted the raw record |

Tests applied:

* `not_null` on `deal_id`
* `unique` on `deal_id`
* `not_null` on `pipeline_id`
* `not_null` on `deal_stage_id`

---

### 3. Intermediate Layer

Model:

```text
int_hubspot_deals_enriched
```

Purpose:

The intermediate model adds business logic to the cleaned deal-level data.

This model keeps the same grain as staging:

```text
One row per HubSpot deal
```

Key logic added:

| Field                      | Description                                                              |
| -------------------------- | ------------------------------------------------------------------------ |
| `deal_status`              | Groups deals into `Open`, `Closed Won`, `Closed Lost`, or `Closed Other` |
| `open_closed_status`       | Simplifies deals into `Open` or `Closed`                                 |
| `effective_amount`         | Best available deal value from HubSpot amount fields                     |
| `weighted_pipeline_amount` | Probability-adjusted pipeline value                                      |
| `created_date`             | Date version of deal creation timestamp                                  |
| `created_month`            | Month when deal was created                                              |
| `close_month`              | Month when deal is expected or marked to close                           |
| `deal_age_days`            | Number of days since deal creation                                       |
| `created_to_close_days`    | Number of days between creation and close date                           |
| `has_amount`               | Indicates whether the deal has an amount                                 |
| `has_company`              | Indicates whether company name is populated                              |
| `has_company_association`  | Indicates whether a company association exists                           |
| `days_since_last_contact`  | Days since the deal was last contacted                                   |

### Effective Amount vs Weighted Pipeline Amount

`effective_amount` answers:

```text
How much is this deal worth based on the best available amount field?
```

It is calculated using a fallback approach:

```sql
coalesce(
    amount,
    amount_in_home_currency,
    closed_amount,
    closed_amount_in_home_currency,
    0
)
```

`weighted_pipeline_amount` answers:

```text
What is the expected value of this deal after applying stage probability?
```

It is calculated as:

```sql
effective_amount * deal_stage_probability
```

Example:

| Deal Amount | Stage Probability | Weighted Pipeline Amount |
| ----------: | ----------------: | -----------------------: |
|      10,000 |              0.10 |                    1,000 |
|      10,000 |              0.50 |                    5,000 |
|      10,000 |              0.90 |                    9,000 |

Tests applied:

* `not_null` on `deal_id`
* `unique` on `deal_id`
* `not_null` on `deal_status`
* `accepted_values` on `deal_status`
* `accepted_values` on `open_closed_status`

---

### 4. Mart Layer

The mart layer creates reporting-ready tables for analysis and dashboarding.

## Mart 1: `mart_sales_pipeline`

Purpose:

Creates a dimensional sales pipeline summary table grouped by month, pipeline, stage, source, status, and project type.

Grain:

```text
One row per created month, close month, pipeline, stage, status, source, and project type
```

Dimensions:

* `created_month`
* `close_month`
* `pipeline_id`
* `deal_stage_id`
* `deal_status`
* `open_closed_status`
* `deal_source`
* `analytics_source`
* `analytics_latest_source`
* `project_type`

Metrics:

| Metric                           | Description                                     |
| -------------------------------- | ----------------------------------------------- |
| `deal_count`                     | Number of deals in the segment                  |
| `open_deal_count`                | Number of open deals                            |
| `closed_deal_count`              | Number of closed deals                          |
| `won_deal_count`                 | Number of closed-won deals                      |
| `lost_deal_count`                | Number of closed-lost deals                     |
| `total_effective_amount`         | Total deal value                                |
| `total_weighted_pipeline_amount` | Total probability-weighted pipeline value       |
| `open_pipeline_amount`           | Total value of open deals                       |
| `weighted_open_pipeline_amount`  | Probability-weighted open pipeline value        |
| `won_amount`                     | Total value of closed-won deals                 |
| `lost_amount`                    | Total value of closed-lost deals                |
| `average_deal_amount`            | Average deal value                              |
| `average_deal_age_days`          | Average age of deals                            |
| `closed_won_rate`                | Closed-won deals divided by total closed deals  |
| `closed_lost_rate`               | Closed-lost deals divided by total closed deals |

---

## Mart 2: `mart_sales_pipeline_monthly`

Purpose:

Creates a simplified executive-level monthly sales pipeline table.

Grain:

```text
One row per created month
```

Metrics:

| Metric                            | Description                                          |
| --------------------------------- | ---------------------------------------------------- |
| `deals_created`                   | Number of deals created in the month                 |
| `open_deals`                      | Number of currently open deals                       |
| `closed_deals`                    | Number of closed deals                               |
| `won_deals`                       | Number of closed-won deals                           |
| `lost_deals`                      | Number of closed-lost deals                          |
| `closed_other_deals`              | Number of closed deals that are neither won nor lost |
| `total_pipeline_value`            | Total deal value                                     |
| `weighted_pipeline_value`         | Probability-weighted deal value                      |
| `average_deal_value`              | Average deal value                                   |
| `average_deal_age_days`           | Average deal age                                     |
| `average_days_since_last_contact` | Average days since last contact                      |
| `win_rate`                        | Closed-won deals divided by total closed deals       |
| `loss_rate`                       | Closed-lost deals divided by total closed deals      |

This table is designed for high-level dashboard reporting.

## dbt Materialization Strategy

The project uses different materializations by layer:

| Layer        | Materialization | Reason                                                     |
| ------------ | --------------- | ---------------------------------------------------------- |
| Staging      | View            | Keeps raw cleaning lightweight and easy to inspect         |
| Intermediate | View            | Keeps business logic reusable without storing extra tables |
| Marts        | Table           | Improves performance for reporting/dashboard use cases     |

Example configuration in `dbt_project.yml`:

```yaml
models:
  dbt_bq_test:
    staging:
      +materialized: view

    intermediate:
      +materialized: view

    marts:
      +materialized: table
```

## Data Tests

This project uses dbt data tests to validate key assumptions.

Test types used:

| Test              | Purpose                                                 |
| ----------------- | ------------------------------------------------------- |
| `not_null`        | Ensures important fields are populated                  |
| `unique`          | Ensures primary keys do not duplicate                   |
| `accepted_values` | Ensures categorical fields only contain expected values |

Examples:

* `deal_id` must be unique and not null.
* `deal_status` must be one of:

  * `Open`
  * `Closed Won`
  * `Closed Lost`
  * `Closed Other`
* `open_closed_status` must be either:

  * `Open`
  * `Closed`

## Key dbt Commands Used

### Parse the project

```bash
dbt parse
```

Checks whether dbt can read and understand the project files.

---

### List dbt models and sources

```bash
dbt ls
```

Lists models, sources, tests, and other dbt resources dbt can detect.

---

### Build a specific model

```bash
dbt run --select stg_hubspot_deals
```

Runs the selected SQL model and creates or updates the object in BigQuery.

---

### Test a specific model

```bash
dbt test --select stg_hubspot_deals
```

Runs the tests defined for the selected model.

---

### Build a model and run tests

```bash
dbt build --select mart_sales_pipeline
```

Builds the selected model and runs its tests.

---

### Build a model and its upstream dependencies

```bash
dbt build --select +mart_sales_pipeline
```

Builds all upstream models first, then builds and tests the selected model.

For this project, that means:

```text
stg_hubspot_deals
        ↓
int_hubspot_deals_enriched
        ↓
mart_sales_pipeline
```

---

### Compile project and write catalog metadata

```bash
dbt compile --write-catalog
```

Used because this project was developed with dbt Fusion / dbt v2 preview, where `dbt docs generate` is not supported.

This writes compiled SQL and catalog metadata into the `target/` folder.

## Validation Checks

After building the marts, validation queries were run in BigQuery to confirm that totals matched between layers.

Example validation:

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

Expected result:

```text
intermediate totals = monthly mart totals
```

All validation checks passed successfully.

## Business Questions Answered

This project supports sales pipeline questions such as:

* How many deals were created each month?
* How much total pipeline value exists?
* How much pipeline value is expected after applying stage probability?
* How many deals are open, closed won, or closed lost?
* What is the monthly win rate?
* What is the average deal value?
* What is the average deal age?
* Which deal sources contribute the most pipeline?
* Which pipeline stages contain the most deal value?
* How complete is the CRM data across key fields?

## Final Outputs

The final reporting tables are:

```text
mart_sales_pipeline
mart_sales_pipeline_monthly
```

These marts are designed to support:

* Sales pipeline dashboards
* Executive monthly reporting
* Deal performance analysis
* Source-level pipeline reporting
* Pipeline value and weighted pipeline analysis

## Key Skills Demonstrated

* dbt project setup
* BigQuery transformation workflow
* Source configuration
* Staging model design
* Intermediate model design
* Mart/modeling layer design
* SQL transformations
* Deduplication logic
* Safe casting
* Business logic modeling
* dbt data tests
* Data validation
* Git/GitHub version control
* Analytics engineering workflow

## Known Limitations

* The project currently uses only raw HubSpot deals data.
* Pipeline stage IDs have not yet been mapped to readable stage names.
* Owner IDs and team IDs have not yet been joined to owner/team dimension tables.
* Deal-company and deal-contact associations are retained as JSON fields but not yet modeled as separate bridge tables.
* Historical deal stage movement is not yet captured with snapshots.
* The project currently focuses on transformation and modeling, not dashboard design.

## Recommended Next Improvements

Planned improvements include:

1. Add a HubSpot pipeline stage mapping seed to convert stage IDs into readable stage names.
2. Create a `dim_hubspot_deal_stage` model.
3. Create bridge models for deal-company and deal-contact associations.
4. Add dbt snapshots to track deal stage changes over time.
5. Add incremental materialization for larger tables.
6. Build a Looker Studio dashboard connected to the final marts.
7. Add more data sources such as HubSpot companies, contacts, owners, and line items.
8. Add exposure definitions for downstream dashboards.

## Project Summary

This project demonstrates how raw HubSpot CRM data can be transformed into reliable, tested, and dashboard-ready analytics models using dbt and BigQuery.

The final pipeline follows a clean analytics engineering structure:

```text
Source → Staging → Intermediate → Marts → Validation
```

The result is a maintainable sales pipeline reporting foundation that can be extended with additional CRM objects, pipeline stage mappings, historical snapshots, and dashboard outputs.

```
```
