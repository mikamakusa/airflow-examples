from __future__ import annotations
import os
from datetime import datetime
import pytest
from airflow import DAG
try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
except ImportError:
    pytest.skip("MSSQL provider not available", allow_module_level=True)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_mssql"
MSSQL_REQUEST = os.environ.get("MSSQL_REQUEST")
MSSQL_ROWS = os.environ.get("MSSQL_ROWS")
MSSQL_TARGET_FIELDS = os.environ.get("MSSQL_TARGET_FIELDS")
MSSQL_TABLE_NAME = os.environ.get("MSSQL_TABLE_NAME")
MSSQL_POPULATE_REQUEST = os.environ.get("MSSQL_POPULATE_REQUEST")


with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 10, 1),
    tags=["example"],
    catchup=False,
) as dag:

    create_table_mssql_task = SQLExecuteQueryOperator(
        task_id="create_country_table",
        conn_id="airflow_mssql",
        sql=MSSQL_REQUEST,
        dag=dag,
    )

    @dag.task(task_id="insert_mssql_task")
    def insert_mssql_hook():
        mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="airflow")
        rows = MSSQL_ROWS
        target_fields = MSSQL_TARGET_FIELDS
        mssql_hook.insert_rows(table=MSSQL_TABLE_NAME, rows=rows, target_fields=target_fields)

    create_table_mssql_from_external_file = SQLExecuteQueryOperator(
        task_id="create_table_from_external_file",
        conn_id="airflow_mssql",
        sql="create_table.sql",
        dag=dag,
    )

    populate_user_table = SQLExecuteQueryOperator(
        task_id="populate_user_table",
        conn_id="airflow_mssql",
        sql=MSSQL_POPULATE_REQUEST,
    )

    (
        create_table_mssql_task
        >> insert_mssql_hook()
        >> create_table_mssql_from_external_file
        >> populate_user_table
    )


    from tests_common.test_utils.watcher import watcher
    list(dag.tasks) >> watcher()
from tests_common.test_utils.system_tests import get_test_run

test_run = get_test_run(dag)