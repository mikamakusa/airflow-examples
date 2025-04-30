from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
SPARK_POOL = os.environ.get("SPARK_POOL")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

SPARK_JOB_PAYLOAD = os.environ.get("SPARK_JOB_PAYLOAD")

with DAG(dag_id="example_synapse_spark_job",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "synapse"]) as dag:
    run_spark_job = AzureSynapseRunSparkBatchOperator(
        task_id="run_spark_job",
        spark_pool=SPARK_POOL,
        payload=SPARK_JOB_PAYLOAD,  # type: ignore
    )