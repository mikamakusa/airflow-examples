from __future__ import annotations
import os
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from airflow.providers.microsoft.azure.sensors.msgraph import MSGraphSensor

DAG_ID = "powerbi"
WORKSPACE_ID = os.environ.get("WORKSPACE_ID")
DATASET_ID = os.environ.get("DATASET_ID")
URL_WORKSPACE = os.environ.get("URL_WORKSPACE")
URL_INFO = os.environ.get("URL_INFO")

with DAG(DAG_ID,start_date=datetime(2021, 1, 1), schedule="@once", tags=["example"]) as dag:
    workspaces = MSGraphAsyncOperator(
        task_id="workspaces",
        conn_id="powerbi",
        url=URL_WORKSPACE,
        result_processor=lambda context, response: list(map(lambda workspace: workspace["id"], response)),
        # type: ignore[typeddict-item, index]
    )

    get_workspace_info = MSGraphAsyncOperator(
        task_id="get_workspace_info",
        conn_id="powerbi",
        url=URL_INFO,
        method="POST",
        query_parameters={
            "lineage": True,
            "datasourceDetails": True,
            "datasetSchema": True,
            "datasetExpressions": True,
            "getArtifactUsers": True,
        },
        data={"workspaces": workspaces.output},
        result_processor=lambda context, response: {"scanId": response["id"]},  # type: ignore[typeddict-item]
    )

    check_workspaces_status = MSGraphSensor.partial(
        task_id="check_workspaces_status",
        conn_id="powerbi_api",
        url="myorg/admin/workspaces/scanStatus/{scanId}",
        timeout=350.0,
    ).expand(path_parameters=get_workspace_info.output)

    refresh_dataset = MSGraphAsyncOperator(
        task_id="refresh_dataset",
        conn_id="powerbi_api",
        url="myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes",
        method="POST",
        path_parameters={
            "workspaceId": WORKSPACE_ID,
            "datasetId": DATASET_ID,
        },
        data={"type": "full"},  # Needed for enhanced refresh
        result_processor=lambda context, response: response["requestid"],  # type: ignore[typeddict-item]
    )

    refresh_dataset_history = MSGraphSensor(
        task_id="refresh_dataset_history",
        conn_id="powerbi_api",
        url="myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes/{refreshId}",
        path_parameters={
            "workspaceId": WORKSPACE_ID,
            "datasetId": DATASET_ID,
            "refreshId": refresh_dataset.output,
        },
        timeout=350.0,
        event_processor=lambda context, event: event["status"] == "Completed",  # type: ignore[typeddict-item]
    )

    workspaces >> get_workspace_info >> check_workspaces_status
    refresh_dataset >> refresh_dataset_history
