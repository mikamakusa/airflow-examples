from __future__ import annotations
from datetime import datetime
from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRecordsShortCircuitOperator,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
import os

DAG_ID = "example_appflow"
ENVIRONMENT_ID = os.environ.get("ENVIRONMENT_ID")
SOURCE_NAME = os.environ.get("SOURCE_NAME")
FLOW_NAME = os.environ.get("FLOW_NAME")

@task
def campaign_dump_full():
    return AppflowRunFullOperator(
        task_id="campaign_dump_full",
        source=SOURCE_NAME,
        flow_name=FLOW_NAME,
    )

@task
def campaign_dump_daily():
    return AppflowRunDailyOperator(
        task_id="campaign_dump_daily",
        source=SOURCE_NAME,
        flow_name=FLOW_NAME,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )

@task
def campaign_dump_before():
    return AppflowRunBeforeOperator(
        task_id="campaign_dump_before",
        source=SOURCE_NAME,
        flow_name=FLOW_NAME,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )

@task
def campaign_dump_after():
    return AppflowRunAfterOperator(
        task_id="campaign_dump_after",
        source=SOURCE_NAME,
        flow_name=FLOW_NAME,
        source_field="LastModifiedDate",
        filter_date="3000-01-01",  # Future date, so no records to dump
    )

@task
def campaign_dump_short_circuit():
    return AppflowRecordsShortCircuitOperator(
        task_id="campaign_dump_short_circuit",
        flow_name=FLOW_NAME,
        appflow_run_task_id="campaign_dump_after",  # Should shortcircuit, no records expected
    )

@dag(DAG_ID,
    schedule="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
)
def appflow():
    should_be_skipped = BashOperator(
        task_id="should_be_skipped",
        bash_command="echo 1",
    )

    chain(campaign_dump_full(),
          campaign_dump_daily(),
          campaign_dump_before(),
          campaign_dump_after(),
          campaign_dump_short_circuit(),
          should_be_skipped,
    )