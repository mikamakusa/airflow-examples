from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.alibaba.cloud.operators.analyticdb_spark import AnalyticDBSparkBatchOperator


SPARK_PI_FILE = os.environ.get("SPARK_PI_FILE")
SPARK_LR_FILE = os.environ.get("SPARK_LR_FILE")
DAG_ID = "spark_job"

@task
def spark_pi():
    return AnalyticDBSparkBatchOperator(
        task_id="task1",
        file=SPARK_PI_FILE,
        class_name="org.apache.spark.examples.SparkPi",
    )

def spark_lr():
    return AnalyticDBSparkBatchOperator(
        task_id="task2",
        file=SPARK_LR_FILE,
        class_name="org.apache.spark.examples.SparkLR",
    )

@dag(DAG_ID, default_args={
        "retries": 1,
        "concurrency": 8,
        "max_active_runs": 8,
    },
    start_date=datetime(2017, 6, 1),
    schedule=None)
def spark_job():
    spark_pi() >> spark_lr()
