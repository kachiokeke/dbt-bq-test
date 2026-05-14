import os
from decimal import Decimal
from typing import Optional

from dotenv import load_dotenv
from google.cloud import bigquery


def get_env_value(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)

    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def run_validation() -> None:
    """
    Validate that the Fake Store staging model reconciles with the mart.

    This checks:
    1. Total product count in staging equals total product count in mart.
    2. Total product value in staging equals total catalog value in mart.
    """
    env_file = os.getenv("ENV_FILE", ".env")
    load_dotenv(env_file, override=True)

    project_id = get_env_value("GCP_PROJECT_ID")
    dbt_dataset_id = os.getenv("DBT_DATASET_ID", "dbt_kachiokeke")

    client = bigquery.Client(project=project_id)

    validation_query = f"""
    select
        'staging' as layer,
        count(*) as total_products,
        round(sum(product_price), 2) as total_catalog_value
    from `{project_id}.{dbt_dataset_id}.stg_fakestore_products`

    union all

    select
        'mart' as layer,
        sum(product_count) as total_products,
        round(sum(total_catalog_value), 2) as total_catalog_value
    from `{project_id}.{dbt_dataset_id}.mart_product_catalog`
    """

    results = list(client.query(validation_query).result())

    if len(results) != 2:
        raise RuntimeError(
            f"Expected 2 validation rows, but got {len(results)} rows."
        )

    validation_data = {row["layer"]: row for row in results}

    staging_products = validation_data["staging"]["total_products"]
    mart_products = validation_data["mart"]["total_products"]

    staging_value = validation_data["staging"]["total_catalog_value"]
    mart_value = validation_data["mart"]["total_catalog_value"]

    staging_value = Decimal(str(staging_value or 0))
    mart_value = Decimal(str(mart_value or 0))

    print("Validation Results")
    print("------------------")
    print(f"Staging product count: {staging_products}")
    print(f"Mart product count:    {mart_products}")
    print(f"Staging catalog value: {staging_value}")
    print(f"Mart catalog value:    {mart_value}")

    if staging_products != mart_products:
        raise RuntimeError(
            "Product count validation failed: "
            f"staging={staging_products}, mart={mart_products}"
        )

    if staging_value != mart_value:
        raise RuntimeError(
            "Catalog value validation failed: "
            f"staging={staging_value}, mart={mart_value}"
        )

    print("Validation passed: staging and mart totals match.")


if __name__ == "__main__":
    run_validation()