from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "/opt/airflow/project"
INGESTION_DIR = f"{PROJECT_DIR}/ingestion/fakestore"
DBT_PROFILES_DIR = "/opt/airflow/project/airflow/dbt_profiles"


default_args = {
    "owner": "kachi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="fakestore_elt_pipeline",
    description="Orchestrates Fake Store API ingestion, BigQuery load, and dbt transformation.",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "bigquery", "dbt", "fakestore"],
) as dag:

    start = EmptyOperator(
        task_id="start_pipeline"
    )

    run_python_ingestion = BashOperator(
        task_id="run_python_ingestion",
        bash_command=f"""
        cd {INGESTION_DIR}
        python extract_products_to_bigquery.py
        """,
    )

    check_fakestore_source_freshness = BashOperator(
        task_id="check_fakestore_source_freshness",
        bash_command=f"""
        cd {PROJECT_DIR}
        dbt source freshness \
          --select source:fakestore_raw.products \
          --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    build_product_catalog_mart = BashOperator(
        task_id="build_product_catalog_mart",
        bash_command=f"""
        cd {PROJECT_DIR}
        dbt build \
          --select +mart_product_catalog \
          --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    finish = EmptyOperator(
        task_id="finish_pipeline"
    )

    (
        start
        >> run_python_ingestion
        >> check_fakestore_source_freshness
        >> build_product_catalog_mart
        >> finish
    )