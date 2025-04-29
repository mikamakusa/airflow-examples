from __future__ import annotations
import os
from datetime import datetime, timedelta
import pytest
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

ALICLOUD_REGION = os.environ.get("ALICLOUD_REGION")
BUCKET_DAG_ID = os.environ.get("BUCKET_DAG_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
OBJECT_KEY = os.environ.get("OBJECT_KEY")
OBJECT_FILE_PATH = os.environ.get("OBJECT_FILE_PATH")

try:
    from airflow.providers.alibaba.cloud.operators.oss import (
        OSSKeySensor,
        OSSCreateBucketOperator,
        OSSDeleteBucketOperator,
        OSSUploadObjectOperator,
        OSSDownloadObjectOperator,
        OSSDeleteBatchObjectOperator,
        OSSDeleteObjectOperator
    )
except ImportError:
    pytest.skip("Alicloud OSS not available", allow_module_level=True)

@task
def oss_create_bucket():
    return OSSCreateBucketOperator(task_id="create_oss_bucket", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME)

@task
def oss_delete_bucket():
    return OSSDeleteObjectOperator(task_id="delete_oss_bucket", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY)

@task
def upload_object():
    return OSSUploadObjectOperator(task_id="upload_oss_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)

@task
def download_object():
    return OSSDownloadObjectOperator(task_id="download_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)

@task
def delete_object():
    return OSSDeleteObjectOperator(task_id="download_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)

@task
def delete_batch():
    return OSSDeleteBatchObjectOperator(task_id="delete_batch_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, keys=[OBJECT_KEY], file_path=OBJECT_FILE_PATH)

@dag(dag_id=BUCKET_DAG_ID, schedule="@daily", start_date=datetime(2021, 1, 1),
     dagrun_timeout=timedelta(minutes=60), tags=["example"], catchup=False)
def oss_bucket():
    chain(
        oss_create_bucket(),
        upload_object(),
        download_object(),
        delete_batch(),
        oss_delete_bucket()
    )