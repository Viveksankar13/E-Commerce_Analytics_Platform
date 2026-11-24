# ecommerce_pricing_orchestrator.py
"""
Airflow Orchestration DAG
Orchestrates the daily backup from local stores to ADLS Gen2, and triggers
the Databricks Medallion Architecture and dynamic pricing notebook run.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Setup basic logging for callbacks
logger = logging.getLogger("airflow.task")


def task_failure_alert(context: Any) -> None:
    """
    Callback alert triggered when a task fails.
    Simulates enterprise paging/email notification systems.
    """
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")
    error = context.get("exception")
    
    logger.error(
        f"CRITICAL: Task '{task_id}' in DAG '{dag_id}' failed on execution date {execution_date}. "
        f"Alert dispatched to administrative team. Exception: {error}"
    )


def task_retry_alert(context: Any) -> None:
    """
    Callback alert triggered when a task is rescheduled for retry.
    """
    task_id = context.get("task_instance").task_id
    retry_number = context.get("task_instance").try_number - 1
    logger.info(
        f"WARNING: Task '{task_id}' failed. Retrying... Attempt number: {retry_number}"
    )


# Default arguments for the DAG (including retry policy and alerts)
default_args = {
    "owner": "airflow-data-eng",
    "depends_on_past": False,
    "email": ["alerts-data-platform@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    # 3 Retries with 5-minute backoff delays
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # Failure alerts
    "on_failure_callback": task_failure_alert,
    "on_retry_callback": task_retry_alert,
}

# Define the DAG
with DAG(
    dag_id="ecommerce_pricing_orchestrator",
    default_args=default_args,
    description="Daily ingestion backup and Databricks lakehouse pricing optimization orchestrator",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Set a 2-hour SLA for the overall pipeline execution
    sla=timedelta(hours=2),
    tags=["ecommerce", "lakehouse", "pricing", "real-time"],
) as dag:

    # Task 1: Execute Python script to backup transactional profiles to ADLS Gen2 landing bucket
    daily_backup_migration = BashOperator(
        task_id="execute_daily_backup",
        # Path inside the Airflow Docker volume mapping
        bash_command="python /opt/airflow/dags/daily_backup.py",
        env={
            "CASSANDRA_HOST": "cassandra",
            "CASSANDRA_PORT": "9042",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "BACKUP_DIR": "/opt/airflow/logs/backups",
            "AZURE_STORAGE_CONNECTION_STRING": "{{ conn.azure_adls_default.extra_dejson.get('connection_string', '') }}"
        },
    )

    # Task 2: Submit a Databricks Job Run via DatabricksSubmitRunOperator
    # Connects using the databricks_default connection (configured with Bearer token authentication)
    databricks_pricing_notebook_run = DatabricksSubmitRunOperator(
        task_id="trigger_databricks_medallion",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "daily_pricing_medallion_pipeline",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",  # Standard Azure cluster node instance
                "num_workers": 2,
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true"
                }
            },
            "notebook_task": {
                "notebook_path": "/Shared/databricks_notebook",
                "base_parameters": {
                    "storage_account_name": "adls2ecommerceanalytics",
                    "environment": "prod",
                    "catalog_name": "main",
                    "schema_name": "ecommerce"
                }
            }
        }
    )

    # Establish linear dependencies
    daily_backup_migration >> databricks_pricing_notebook_run
