from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "aci"
CONTAINER_REGISTRY_SERVER = os.environ.get("CONTAINER_REGISTRY_SERVER")
AZURE_VOLUME_SHARE_NAME = os.environ.get("AZURE_VOLUME_SHARE_NAME")
AZURE_STORAGE_ACOOUNT = os.environ.get("AZURE_STORAGE_ACOOUNT")

@task
def start_container():
    return AzureContainerInstancesOperator(
        ci_conn_id="azure_default",
        registry_conn_id=None,
        resource_group="resource-group",
        name="aci-test-{{ ds }}",
        image="hello-world",
        region="WestUS2",
        environment_variables={},
        volumes=[
            (
                "azure_container_volume_default",
                AZURE_STORAGE_ACOOUNT,
                AZURE_VOLUME_SHARE_NAME,
                "/home",
                True,
            )
        ],
        memory_in_gb=4.0,
        cpu=1.0,
        task_id="start_container",
    )


@dag(dag_id=DAG_ID, default_args={"retries": 1}, schedule=timedelta(days=1),
     start_date=datetime(2018, 11, 1), catchup=False, tags=["example"])
def container():
    start_container()