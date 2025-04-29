from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
SPARK_POOL = os.environ.get("SPARK_POOL")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

SPARK_JOB_PAYLOAD = {
    "name": "SparkJob",
    "file": "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/wordcount.py",
    "args": [
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/shakespeare.txt",
        "abfss://spark@providersstorageaccgen2.dfs.core.windows.net/results/",
    ],
    "jars": [],
    "pyFiles": [],
    "files": [],
    "conf": {
        "spark.dynamicAllocation.enabled": "false",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "2",
    },
    "numExecutors": 2,
    "executorCores": 4,
    "executorMemory": "28g",
    "driverCores": 4,
    "driverMemory": "28g",
}

@task
def run_spark_job():
    return AzureSynapseRunSparkBatchOperator(
        task_id="run_spark_job",
        spark_pool=SPARK_POOL,
        payload=SPARK_JOB_PAYLOAD,  # type: ignore
    )

@dag(dag_id="example_synapse_spark_job",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "synapse"])
def spark():
    run_spark_job()