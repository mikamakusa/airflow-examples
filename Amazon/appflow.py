from __future__ import annotations
from datetime import datetime
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRecordsShortCircuitOperator,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
)
from airflow.providers.standard.operators.bash import BashOperator

DAG_ID = "example_appflow"
ENV_ID = "example_appflow_env"

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    source_name = "salesforce"
    flow_name = f"{ENV_ID}-salesforce-campaign"

    # [START howto_operator_appflow_run_full]
    campaign_dump_full = AppflowRunFullOperator(
        task_id="campaign_dump_full",
        source=source_name,
        flow_name=flow_name,
    )
    # [END howto_operator_appflow_run_full]

    # [START howto_operator_appflow_run_daily]
    campaign_dump_daily = AppflowRunDailyOperator(
        task_id="campaign_dump_daily",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )
    # [END howto_operator_appflow_run_daily]

    # [START howto_operator_appflow_run_before]
    campaign_dump_before = AppflowRunBeforeOperator(
        task_id="campaign_dump_before",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="{{ ds }}",
    )
    # [END howto_operator_appflow_run_before]

    # [START howto_operator_appflow_run_after]
    campaign_dump_after = AppflowRunAfterOperator(
        task_id="campaign_dump_after",
        source=source_name,
        flow_name=flow_name,
        source_field="LastModifiedDate",
        filter_date="3000-01-01",  # Future date, so no records to dump
    )
    # [END howto_operator_appflow_run_after]

    # [START howto_operator_appflow_shortcircuit]
    campaign_dump_short_circuit = AppflowRecordsShortCircuitOperator(
        task_id="campaign_dump_short_circuit",
        flow_name=flow_name,
        appflow_run_task_id="campaign_dump_after",  # Should shortcircuit, no records expected
    )
    # [END howto_operator_appflow_shortcircuit]

    should_be_skipped = BashOperator(
        task_id="should_be_skipped",
        bash_command="echo 1",
    )

    chain(
        campaign_dump_full,
        campaign_dump_daily,
        campaign_dump_before,
        campaign_dump_after,
        campaign_dump_short_circuit,
        should_be_skipped,
    )