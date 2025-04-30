from __future__ import annotations
import os
from datetime import datetime
from airflow.decorators import task, dag
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.adls import ADLSListOperator

LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH", "localfile.txt")
REMOTE_FILE_PATH = os.environ.get("REMOTE_LOCAL_PATH", "remote.txt")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PATH = os.environ.get("PATH")
DAG_ID = "example_adls_list"

with DAG(DAG_ID, start_date=datetime(2021, 1, 1), schedule=None, tags=["example"]) as dag:
    adls_files = ADLSListOperator(
        task_id="adls_files",
        path=PATH,
        azure_data_lake_conn_id="azure_data_lake_default",
    )
    adls_files()