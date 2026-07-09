import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "pprojects-488010")
RAW_FAKESTORE_DATASET = os.getenv("RAW_FAKESTORE_DATASET", "raw_fakestore")
HUBSPOT_RAW_DATASET = os.getenv(
    "HUBSPOT_RAW_DATASET",
    "pprojects_488010_marketing_data",
)
DBT_DATASET = os.getenv("DBT_DATASET", "dbt_kachiokeke")
SNAPSHOTS_DATASET = os.getenv("SNAPSHOTS_DATASET", "snapshots")

OUTPUT_PATH = Path(__file__).resolve().parent / "pipeline_context.json"


TABLES_TO_CHECK = [
    {
        "name": "raw_fakestore.products",
        "dataset": RAW_FAKESTORE_DATASET,
        "table": "products",
        "layer": "raw",
        "critical": True,
    },
    {
        "name": "raw_fakestore.ingestion_run_log",
        "dataset": RAW_FAKESTORE_DATASET,
        "table": "ingestion_run_log",
        "layer": "ops",
        "critical": False,
    },
    {
        "name": "pprojects_488010_marketing_data.deals",
        "dataset": HUBSPOT_RAW_DATASET,
        "table": "deals",
        "layer": "raw",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.stg_fakestore_products",
        "dataset": DBT_DATASET,
        "table": "stg_fakestore_products",
        "layer": "staging",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.mart_product_catalog",
        "dataset": DBT_DATASET,
        "table": "mart_product_catalog",
        "layer": "mart",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.stg_hubspot_deals",
        "dataset": DBT_DATASET,
        "table": "stg_hubspot_deals",
        "layer": "staging",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.int_hubspot_deals_enriched",
        "dataset": DBT_DATASET,
        "table": "int_hubspot_deals_enriched",
        "layer": "intermediate",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.mart_sales_pipeline",
        "dataset": DBT_DATASET,
        "table": "mart_sales_pipeline",
        "layer": "mart",
        "critical": True,
    },
    {
        "name": "dbt_kachiokeke.mart_sales_pipeline_monthly",
        "dataset": DBT_DATASET,
        "table": "mart_sales_pipeline_monthly",
        "layer": "mart",
        "critical": True,
    },
    {
        "name": "snapshots.snap_hubspot_deals",
        "dataset": SNAPSHOTS_DATASET,
        "table": "snap_hubspot_deals",
        "layer": "snapshot",
        "critical": False,
    },
]


def fully_qualified_table(table_config: dict[str, Any]) -> str:
    return f"{PROJECT_ID}.{table_config['dataset']}.{table_config['table']}"


def run_scalar_query(client: bigquery.Client, query: str) -> Any:
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        return None

    return rows[0][0]


def get_table_row_count(
    client: bigquery.Client,
    table_config: dict[str, Any],
) -> dict[str, Any]:
    table_id = fully_qualified_table(table_config)
    query = f"select count(*) as row_count from `{table_id}`"

    result = {
        "name": table_config["name"],
        "project_id": PROJECT_ID,
        "dataset": table_config["dataset"],
        "table": table_config["table"],
        "full_table_id": table_id,
        "layer": table_config["layer"],
        "critical": table_config["critical"],
        "exists": False,
        "row_count": None,
        "error": None,
    }

    try:
        result["row_count"] = int(run_scalar_query(client, query))
        result["exists"] = True
        print(f"OK: {table_config['name']} has {result['row_count']} rows.")
    except GoogleAPIError as error:
        result["error"] = str(error)
        print(f"WARN: Could not count {table_config['name']}: {error}")
    except Exception as error:
        result["error"] = str(error)
        print(f"WARN: Unexpected error counting {table_config['name']}: {error}")

    return result


def serialize_bigquery_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    return value


def collect_latest_ingestion_run(client: bigquery.Client) -> dict[str, Any]:
    table_id = f"{PROJECT_ID}.{RAW_FAKESTORE_DATASET}.ingestion_run_log"
    query = f"""
    select
        run_id,
        source_name,
        target_table,
        rows_loaded,
        started_at,
        completed_at,
        status,
        error_message
    from `{table_id}`
    order by completed_at desc
    limit 1
    """

    empty_result = {
        "available": False,
        "record": None,
        "error": None,
    }

    try:
        rows = list(client.query(query).result())

        if not rows:
            print("INFO: ingestion_run_log exists, but no runs were found.")
            return empty_result

        record = {
            key: serialize_bigquery_value(value)
            for key, value in dict(rows[0]).items()
        }

        print(
            "OK: Latest ingestion run collected "
            f"with status={record.get('status')}."
        )

        return {
            "available": True,
            "record": record,
            "error": None,
        }
    except GoogleAPIError as error:
        print(f"WARN: Could not collect latest ingestion run: {error}")
        return {
            **empty_result,
            "error": str(error),
        }
    except Exception as error:
        print(f"WARN: Unexpected error collecting latest ingestion run: {error}")
        return {
            **empty_result,
            "error": str(error),
        }


def build_context() -> dict[str, Any]:
    print(f"Using BigQuery project: {PROJECT_ID}")
    print("Collecting read-only pipeline context.")

    client = bigquery.Client(project=PROJECT_ID)

    tables = [
        get_table_row_count(client, table_config)
        for table_config in TABLES_TO_CHECK
    ]

    latest_ingestion_run = collect_latest_ingestion_run(client)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_id": PROJECT_ID,
        "datasets": {
            "raw_fakestore": RAW_FAKESTORE_DATASET,
            "hubspot_raw": HUBSPOT_RAW_DATASET,
            "dbt": DBT_DATASET,
            "snapshots": SNAPSHOTS_DATASET,
        },
        "tables": tables,
        "latest_ingestion_run": latest_ingestion_run,
    }


def main() -> None:
    context = build_context()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(context, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"Wrote pipeline context to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
