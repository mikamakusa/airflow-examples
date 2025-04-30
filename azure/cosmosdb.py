from __future__ import annotations
import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.cosmos import AzureCosmosInsertDocumentOperator
from airflow.providers.microsoft.azure.sensors.cosmos import AzureCosmosDocumentSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
DOCUMENT_ID = os.environ.get("DOCUMENT_ID")
DAG_ID = "example_azure_cosmosdb_sensor"

with DAG(dag_id=DAG_ID, default_args={"database_name": "airflow_example_db"},
    start_date=datetime(2021, 1, 1), schedule=None, catchup=False, doc_md=__doc__, tags=["example"]) as dag:
    check_cosmos_file = AzureCosmosDocumentSensor(
        task_id="check_cosmos_file",
        collection_name=COLLECTION_NAME,
        document_id="airflow_checkid",
        database_name=DATABASE_NAME,
    )
    insert_cosmosdb_file = AzureCosmosInsertDocumentOperator(
        task_id="insert_cosmos_file",
        collection_name=COLLECTION_NAME,
        document={"id": DATABASE_NAME},
        database_name=DOCUMENT_ID,
    )

    check_cosmos_file >> insert_cosmosdb_file