import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery


def extract_products() -> list[dict]:
    """
    Extract product data from the Fake Store API.
    """
    url = "https://fakestoreapi.com/products"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    products = response.json()

    if not isinstance(products, list):
        raise ValueError("Expected API response to be a list of products.")

    return products


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


def main() -> None:
    load_dotenv()

    products = extract_products()
    df = transform_products_for_raw_load(products)
    load_to_bigquery(df)


if __name__ == "__main__":
    main()