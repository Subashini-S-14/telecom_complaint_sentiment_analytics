from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ---------------- DEFAULT CONFIG ----------------
default_args = {
    "owner": "Subashini",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------- DAG DEFINITION ----------------
with DAG(
    dag_id="telecom_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["telecom", "spark", "medallion"],
    description="End-to-End Telecom Data Pipeline (Bronze → Silver → Gold → Warehouse)"
) as dag:

    # ---------------- TASK 1: BRONZE (PLACEHOLDER) ----------------
    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="echo 'Bronze ingestion handled externally (API pipeline)'"
    )

    # ---------------- TASK 2: SILVER ----------------
    silver = BashOperator(
        task_id="silver_processing",
        bash_command="docker exec telecom_spark_engine python /workspace/cleaning/transformation.py"
    )

    # ---------------- TASK 3: GOLD ----------------
    gold = BashOperator(
        task_id="gold_processing",
        bash_command="docker exec telecom_spark_engine python /workspace/cleaning/curating.py"
    )

    # ---------------- TASK 4: WAREHOUSE ----------------
    warehouse = BashOperator(
        task_id="warehouse_processing",
        bash_command="docker exec telecom_spark_engine python /workspace/processing/star_schema.py"
    )

    # ---------------- PIPELINE FLOW ----------------
    bronze >> silver >> gold >> warehouse
