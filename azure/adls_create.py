from __future__ import annotations
import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.adls import ADLSCreateObjectOperator, ADLSDeleteOperator

REMOTE_FILE_PATH = os.environ.get("REMOTE_LOCAL_PATH", "remote.txt")
DATA = os.environ.get("DATA")
DAG_ID = "adls_create"

with DAG(DAG_ID, start_date=datetime(2021, 1, 1), catchup=False, schedule=None, tags=["example"]) as dag:
    upload_data = ADLSCreateObjectOperator(
        task_id="upload_data",
        file_system_name="Fabric",
        file_name=REMOTE_FILE_PATH,
        data=DATA,
        replace=True,
    )
    delete_data = ADLSDeleteOperator(task_id="remove_task", path=REMOTE_FILE_PATH, recursive=True)
    upload_data >> delete_data