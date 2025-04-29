from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "jdbc_operator"

@task
def run():
    return EmptyOperator(task_id="run")

@task
def delete():
    return SQLExecuteQueryOperator(
        task_id="delete",
        sql="delete from my_schema.my_table where dt = {{ ds }}",
        conn_id="my_jdbc_connection",
        autocommit=True
    )

def insert():
    return SQLExecuteQueryOperator(
        task_id="insert",
        sql="insert into my_schema.my_table select dt, value from my_schema.source_data",
        conn_id="my_jdbc_connection",
        autocommit=True,
    )

@dag(dag_id=DAG_ID, schedule="0 0 * * *", start_date=datetime(2021, 1, 1),
     dagrun_timeout=timedelta(minutes=60), tags=["example"], catchup=False)
def execute_query():
    delete() >> insert() >> run()