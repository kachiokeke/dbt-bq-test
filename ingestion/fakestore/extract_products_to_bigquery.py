import os
import traceback
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery


def extract_products() -> list[dict]:
    """
    Extract product data from a public ecommerce API.

    Primary source: Fake Store API.
    Fallback source: DummyJSON products API.
    """
    api_sources = [
        {
            "name": "fakestoreapi_products",
            "url": "https://fakestoreapi.com/products",
            "type": "list",
        },
        {
            "name": "dummyjson_products",
            "url": "https://dummyjson.com/products?limit=100",
            "type": "dummyjson",
        },
    ]

    last_error = None

    for source in api_sources:
        try:
            response = requests.get(
                source["url"],
                timeout=30,
                headers={
                    "User-Agent": "Mozilla/5.0 dbt-bigquery-elt-project"
                },
            )
            response.raise_for_status()

            data = response.json()

            if source["type"] == "list":
                if not isinstance(data, list):
                    raise ValueError("Expected Fake Store API response to be a list.")
                return data

            if source["type"] == "dummyjson":
                products = data.get("products", [])
                if not isinstance(products, list):
                    raise ValueError("Expected DummyJSON response to contain a products list.")

                normalized_products = []

                for product in products:
                    normalized_products.append(
                        {
                            "id": product.get("id"),
                            "title": product.get("title"),
                            "price": product.get("price"),
                            "description": product.get("description"),
                            "category": product.get("category"),
                            "image": product.get("thumbnail"),
                            "rating": {
                                "rate": product.get("rating"),
                                "count": product.get("stock"),
                            },
                        }
                    )

                return normalized_products

        except Exception as error:
            last_error = error
            print(f"Failed to extract from {source['name']}: {error}")

    raise RuntimeError(f"All product API sources failed. Last error: {last_error}")


def transform_products_for_raw_load(products: list[dict]) -> pd.DataFrame:
    """
    Convert raw API JSON into a BigQuery-loadable dataframe.

    This is still treated as raw-ish data. We only flatten obvious nested fields
    and add ingestion metadata. Proper business cleaning will happen in dbt.
    """
    extracted_at = datetime.now(timezone.utc)

    records = []

    for product in products:
        rating = product.get("rating") or {}

        records.append(
            {
                "id": product.get("id"),
                "title": product.get("title"),
                "price": product.get("price"),
                "description": product.get("description"),
                "category": product.get("category"),
                "image": product.get("image"),
                "rating_rate": rating.get("rate"),
                "rating_count": rating.get("count"),
                "_ingested_at": extracted_at,
                "_source": "fakestoreapi_products",
            }
        )

    return pd.DataFrame(records)


def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Load dataframe into BigQuery using WRITE_TRUNCATE for the first version
    of this ingestion pipeline.
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_TABLE_ID")

    if not project_id or not dataset_id or not table_id:
        raise ValueError(
            "Missing one or more required environment variables: "
            "GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID"
        )

    client = bigquery.Client(project=project_id)

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(
        df,
        full_table_id,
        job_config=job_config,
    )

    load_job.result()

    destination_table = client.get_table(full_table_id)

    print(f"Loaded {destination_table.num_rows} rows into {full_table_id}")


def write_ingestion_log(
    run_id: str,
    source_name: str,
    target_table: str,
    rows_loaded: int,
    started_at: datetime,
    completed_at: datetime,
    status: str,
    error_message: Optional[str] = None,) -> None:
    """
    Write an ingestion run log record to BigQuery.
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")

    if not project_id or not dataset_id:
        raise ValueError(
            "Missing one or more required environment variables: "
            "GCP_PROJECT_ID, BQ_DATASET_ID"
        )

    client = bigquery.Client(project=project_id)

    log_table_id = f"{project_id}.{dataset_id}.ingestion_run_log"

    log_record = [
        {
            "run_id": run_id,
            "source_name": source_name,
            "target_table": target_table,
            "rows_loaded": rows_loaded,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "status": status,
            "error_message": error_message,
        }
    ]

    errors = client.insert_rows_json(log_table_id, log_record)

    if errors:
        raise RuntimeError(f"Failed to write ingestion log: {errors}")


def main() -> None:
    env_file = os.getenv("ENV_FILE", ".env")
    load_dotenv(env_file, override=True)

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_TABLE_ID")

    target_table = f"{project_id}.{dataset_id}.{table_id}"
    rows_loaded = 0

    try:
        products = extract_products()
        df = transform_products_for_raw_load(products)

        rows_loaded = len(df)

        print(f"Extracted {rows_loaded} product records from API.")

        load_to_bigquery(df)

        completed_at = datetime.now(timezone.utc)

        write_ingestion_log(
            run_id=run_id,
            source_name="public_product_api",
            target_table=target_table,
            rows_loaded=rows_loaded,
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            error_message=None,
        )

    except Exception as error:
        completed_at = datetime.now(timezone.utc)
        error_message = traceback.format_exc()

        try:
            write_ingestion_log(
                run_id=run_id,
                source_name="public_product_api",
                target_table=target_table,
                rows_loaded=rows_loaded,
                started_at=started_at,
                completed_at=completed_at,
                status="failed",
                error_message=error_message,
            )
        except Exception as log_error:
            print(f"Failed to write failure log: {log_error}")

        raise error


if __name__ == "__main__":
    main()