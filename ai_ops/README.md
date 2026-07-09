# Optional AI/DataOps Summary Layer

This folder contains an optional reporting layer for the dbt + BigQuery ELT
project. It does not replace ingestion, dbt models, dbt tests, source freshness,
Airflow, or GitHub Actions. It reads pipeline metadata and produces a Markdown
summary that is easier to review after a run.

## What it does

The layer has two scripts:

- `collect_pipeline_context.py` reads BigQuery row counts and the latest Fake
  Store ingestion run, then writes `ai_ops/pipeline_context.json`.
- `generate_pipeline_summary.py` reads that JSON file and writes
  `reports/latest_pipeline_summary.md`.

The BigQuery operations are read-only. The scripts do not write, truncate,
overwrite, or mutate warehouse tables.

## Why it sits after deterministic checks

The existing deterministic checks are still the source of truth:

- dbt tests validate model-level assumptions.
- dbt source freshness validates source recency.
- Python validation reconciles Fake Store staging and mart totals.
- CI checks that the dbt project parses and compiles.

This optional layer is intended for explanation and triage after those checks
run. It gives a concise operational view of row counts, ingestion status, and
simple warning rules.

## Why AI should explain, not replace tests

AI-generated text is useful for summarizing pipeline health, highlighting
possible causes, and making results easier for humans to scan. It should not be
used as the authority for data correctness.

dbt tests, source freshness checks, validation queries, and warehouse metadata
are deterministic and reproducible. The summary should explain those signals,
not override them.

## Run locally

From the repository root:

```bash
python ai_ops/collect_pipeline_context.py
python ai_ops/generate_pipeline_summary.py
```

The collector uses these environment variables, with defaults:

```bash
GCP_PROJECT_ID=pprojects-488010
RAW_FAKESTORE_DATASET=raw_fakestore
HUBSPOT_RAW_DATASET=pprojects_488010_marketing_data
DBT_DATASET=dbt_kachiokeke
SNAPSHOTS_DATASET=snapshots
```

Authentication is handled by the normal BigQuery Python client environment,
such as application default credentials or an externally configured credential
path. Do not commit credential files or `.env` files.

## Future extensions

The deterministic Markdown report can later be routed to Slack or email as a
post-run artifact. The generator also includes a placeholder function where a
future LLM rewrite step could improve tone or produce an executive summary.

If an LLM step is added, keep the rule-based summary as the source of truth and
make the LLM rewrite optional.
