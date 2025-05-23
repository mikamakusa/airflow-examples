from __future__ import annotations
import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.adls import ADLSDeleteOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator

LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH", "localfile.txt")
REMOTE_FILE_PATH = os.environ.get("REMOTE_LOCAL_PATH", "remote.txt")

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "adls_delete"

with DAG (DAG_ID, start_date=datetime(2021, 1, 1), schedule=None, tags=["example"]) as dag:
    upload_file = LocalFilesystemToADLSOperator(
        task_id="upload_task",
        local_path=LOCAL_FILE_PATH,
        remote_path=REMOTE_FILE_PATH,
    )
    remove_file = ADLSDeleteOperator(task_id="delete_task", path=REMOTE_FILE_PATH, recursive=True)
    upload_file >> remove_file