"""
Healthcare Data Pipeline DAG
=============================
This DAG is designed to run on Google Cloud Composer 2.x (managed Airflow 2.x).

Requirements for deployment:
    - Cloud Composer environment with Airflow 2.x
    - apache-airflow-providers-google installed (included by default in Composer)
    - dbt-bigquery installed in Composer environment
    - elementary-data installed in Composer environment
    - GCP service account with BigQuery, GCS, and Dataproc permissions

Local IDE may show unresolved import warnings for airflow providers —
this is expected since Airflow is not installed in the local virtual environment.
All imports resolve correctly inside the Cloud Composer runtime environment.

To test locally, use Astro CLI:
    https://docs.astronomer.io/astro/cli/install-cli
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import uuid

# ── Default args ─────────────────────────────────────────
default_args = {
    "owner": "healthcare-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
}

# ── Config ───────────────────────────────────────────────
GCP_PROJECT_ID = "covid-data-observability"
GCP_REGION = "us-central1"
GCS_BUCKET = "healthcare-data-platform-safiya"
BQ_DATASET = "healthcare_staging"

# ── DAG ──────────────────────────────────────────────────
with DAG(
    dag_id="healthcare_data_pipeline",
    description="End-to-end healthcare data quality and observability pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "pyspark", "dbt", "gx", "elementary"],
) as dag:

    # ── Task 1: Extract raw data via PySpark ─────────────
    extract_covid_data = DataprocCreateBatchOperator(
        task_id="extract_covid_data",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/extract_covid_data.py",
                "args": [GCP_PROJECT_ID, GCS_BUCKET],
            },
            "runtime_config": {"version": "2.1"},
        },
        batch_id=f"extract-covid-{uuid.uuid4().hex[:8]}",
    )

    # ── Task 2: Load Parquet to BigQuery via PySpark ─────
    load_to_bigquery = DataprocCreateBatchOperator(
        task_id="load_to_bigquery",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/load_to_bigquery.py",
                "args": [GCP_PROJECT_ID, GCS_BUCKET],
            },
            "runtime_config": {"version": "2.1"},
        },
        batch_id=f"load-bq-{uuid.uuid4().hex[:8]}",
    )

    # ── Task 3: Validate raw data in BigQuery ────────────
    validate_raw_data = BigQueryCheckOperator(
        task_id="validate_raw_data",
        sql=f"""
            SELECT COUNT(*) > 0 
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.covid19_raw`
            WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """,
        use_legacy_sql=False,
        location="US",
    )

    # ── Task 4: Run dbt models ───────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            cd /home/airflow/gcs/dags/healthcare_dbt &&
            dbt run --target dev --profiles-dir /home/airflow/gcs/dags/profiles
        """,
    )

    # ── Task 5: Run dbt tests ────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd /home/airflow/gcs/dags/healthcare_dbt &&
            dbt test --target dev --profiles-dir /home/airflow/gcs/dags/profiles
        """,
    )

    # ── Task 6: Run Great Expectations validations ───────
    gx_validation = BashOperator(
        task_id="gx_validation",
        bash_command=f"""
            cd /home/airflow/gcs/dags &&
            python src/quality/run_validations.py
        """,
    )

    # ── Task 7: Generate Elementary report ───────────────
    elementary_report = BashOperator(
        task_id="elementary_report",
        bash_command=f"""
            cd /home/airflow/gcs/dags/healthcare_dbt &&
            edr report --profiles-dir /home/airflow/gcs/dags/profiles
        """,
    )

    # ── Task dependencies ────────────────────────────────
    (
        extract_covid_data
        >> load_to_bigquery
        >> validate_raw_data
        >> dbt_run
        >> dbt_test
        >> gx_validation
        >> elementary_report
    )