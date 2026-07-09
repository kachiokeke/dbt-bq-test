# Latest Pipeline Summary

Generated at: `2026-07-09T18:35:25.268071+00:00`
Context collected at: `2026-07-09T18:18:46.084428+00:00`
Project: `pprojects-488010`

## Overall Pipeline Status

Healthy based on available row-count checks

## Source Health

- `raw_fakestore.products`: available with 100 rows
- `raw_fakestore.ingestion_run_log`: available with 17 rows
- `pprojects_488010_marketing_data.deals`: available with 161 rows

## Row Counts by Table

| Table | Layer | Exists | Row Count |
|---|---:|---:|---:|
| `raw_fakestore.products` | raw | Yes | 100 |
| `raw_fakestore.ingestion_run_log` | ops | Yes | 17 |
| `pprojects_488010_marketing_data.deals` | raw | Yes | 161 |
| `dbt_kachiokeke.stg_fakestore_products` | staging | Yes | 100 |
| `dbt_kachiokeke.mart_product_catalog` | mart | Yes | 11 |
| `dbt_kachiokeke.stg_hubspot_deals` | staging | Yes | 161 |
| `dbt_kachiokeke.int_hubspot_deals_enriched` | intermediate | Yes | 161 |
| `dbt_kachiokeke.mart_sales_pipeline` | mart | Yes | 147 |
| `dbt_kachiokeke.mart_sales_pipeline_monthly` | mart | Yes | 15 |
| `snapshots.snap_hubspot_deals` | snapshot | Yes | 265 |

## Latest Ingestion Run

- Run ID: `43fd5fcb-36dd-4676-879e-cfc4a097563a`
- Source: `public_product_api`
- Target table: `pprojects-488010.raw_fakestore.products`
- Status: `success`
- Rows loaded: `100`
- Started at: `2026-07-06T11:39:59.069546+00:00`
- Completed at: `2026-07-06T11:40:02.211431+00:00`

## Warnings

- No warnings from deterministic row-count rules.

## Recommended Next Actions

- Continue using dbt tests, source freshness, and validation as the primary quality gates.
- Review this summary after scheduled runs to confirm source-to-mart row movement.
- Consider routing this Markdown report to Slack or email for lightweight monitoring.
