from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "aci"
CONTAINER_REGISTRY_SERVER = os.environ.get("CONTAINER_REGISTRY_SERVER")
AZURE_VOLUME_SHARE_NAME = os.environ.get("AZURE_VOLUME_SHARE_NAME")
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACOOUNT")
AZURE_RESOURCE_GROUP = os.environ.get("AZURE_RESOURCE_GROUP")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME")
AZURE_REGION = os.environ.get("AZURE_REGION")
AZURE_CONTAINER_IMAGE = os.environ.get("AZURE_CONTAINER_IMAGE")
CONTAINER_MEMORY = os.environ.get("CONTAINER_MEMORY")
CONTAINER_CPU = os.environ.get("CONTAINER_CPU")

with DAG(dag_id=DAG_ID, default_args={"retries": 1}, schedule=timedelta(days=1),
         tart_date=datetime(2018, 11, 1), catchup=False, tags=["example"]) as dag:
    start_container = AzureContainerInstancesOperator(
        ci_conn_id="azure_default",
        registry_conn_id=None,
        resource_group=AZURE_RESOURCE_GROUP,
        name=AZURE_CONTAINER_NAME,
        image=AZURE_CONTAINER_IMAGE,
        region=AZURE_REGION,
        environment_variables={},
        volumes=[
            (
                "azure_container_volume_default",
                AZURE_STORAGE_ACCOUNT,
                AZURE_VOLUME_SHARE_NAME,
                "/home",
                True,
            )
        ],
        memory_in_gb=CONTAINER_MEMORY,
        cpu=CONTAINER_CPU,
        task_id="start_container",
    )