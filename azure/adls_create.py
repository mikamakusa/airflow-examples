from __future__ import annotations
import os
from datetime import datetime
from airflow.decorators import dag,task
from airflow.providers.microsoft.azure.operators.adls import ADLSCreateObjectOperator, ADLSDeleteOperator

REMOTE_FILE_PATH = os.environ.get("REMOTE_LOCAL_PATH", "remote.txt")
DATA = os.environ.get("DATA")
DAG_ID = "adls_create"

@task
def upload_data():
    return ADLSCreateObjectOperator(
        task_id="upload_data",
        file_system_name="Fabric",
        file_name=REMOTE_FILE_PATH,
        data=DATA,
        replace=True,
    )

@task
def delete_data():
    return ADLSDeleteOperator(task_id="remove_task", path=REMOTE_FILE_PATH, recursive=True)

@dag(start_date=datetime(2021, 1, 1), catchup=False, schedule=None, tags=["example"])
def adls_create():
    upload_data() >> delete_data()