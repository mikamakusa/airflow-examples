from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.models.dag import DAG
from airflow.providers.alibaba.cloud.operators.analyticdb_spark import AnalyticDBSparkBatchOperator


SPARK_PI_FILE = os.environ.get("SPARK_PI_FILE")
SPARK_LR_FILE = os.environ.get("SPARK_LR_FILE")
SPARK_CLUSTER_ID = os.environ.get("SPARK_CLUSTER_ID")
ALIBABA_RG_NAME = os.environ.get("ALIBABA_RG_NAME")
DAG_ID = "spark_job"

default_args = {
    "retries": 1,
    "concurrency": 8,
    "max_active_runs": 8,
}

with DAG(DAG_ID, default_args, start_date=datetime(2017, 6, 1), schedule=None) as dag:
    spark_pi = AnalyticDBSparkBatchOperator(task_id="task1", file=SPARK_PI_FILE,
                                            class_name="org.apache.spark.examples.SparkPi",
                                            cluster_id=SPARK_CLUSTER_ID,rg_name=ALIBABA_RG_NAME)
    spark_lr = AnalyticDBSparkBatchOperator(task_id="task2", file=SPARK_LR_FILE,
                                            class_name="org.apache.spark.examples.SparkLR",
                                            cluster_id=SPARK_CLUSTER_ID,rg_name=ALIBABA_RG_NAME
    )
    spark_pi >> spark_lr
