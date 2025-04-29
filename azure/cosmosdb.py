from __future__ import annotations
import os
from datetime import datetime
from airflow.decorators import task,dag
from airflow.providers.microsoft.azure.operators.cosmos import AzureCosmosInsertDocumentOperator
from airflow.providers.microsoft.azure.sensors.cosmos import AzureCosmosDocumentSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
DOCUMENT_ID = os.environ.get("DOCUMENT_ID")
DAG_ID = "example_azure_cosmosdb_sensor"

@task
def check_cosmos_file():
    return AzureCosmosDocumentSensor(
        task_id="check_cosmos_file",
        collection_name=COLLECTION_NAME,
        document_id="airflow_checkid",
        database_name=DATABASE_NAME,
    )

def insert_cosmosdb_file():
    return AzureCosmosInsertDocumentOperator(
        task_id="insert_cosmos_file",
        collection_name=COLLECTION_NAME,
        document={"id": DATABASE_NAME},
        database_name=DOCUMENT_ID,
    )

@dag(dag_id=DAG_ID, default_args={"database_name": "airflow_example_db"},
    start_date=datetime(2021, 1, 1), schedule=None, catchup=False, doc_md=__doc__, tags=["example"])
def cosmosdb_sensor():
    check_cosmos_file() >> insert_cosmosdb_file()