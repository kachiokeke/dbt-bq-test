import os
from datetime import datetime, timezone

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


def main() -> None:
    load_dotenv()

    products = extract_products()
    df = transform_products_for_raw_load(products)
    load_to_bigquery(df)


if __name__ == "__main__":
    main()