from __future__ import annotations
import os
from datetime import datetime, timedelta
import pytest
from airflow.models.dag import DAG

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

with DAG(dag_id=BUCKET_DAG_ID, schedule="@daily", start_date=datetime(2021, 1, 1),
     dagrun_timeout=timedelta(minutes=60), tags=["example"], catchup=False) as dag:
    oss_create_bucket = OSSCreateBucketOperator(task_id="create_oss_bucket", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME)
    oss_delete_bucket = OSSDeleteObjectOperator(task_id="delete_oss_bucket", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY)
    upload_object = OSSUploadObjectOperator(task_id="upload_oss_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)
    download_object = OSSDownloadObjectOperator(task_id="download_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)
    delete_object = OSSDeleteObjectOperator(task_id="download_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, key=OBJECT_KEY, file_path=OBJECT_FILE_PATH)
    delete_batch = OSSDeleteBatchObjectOperator(task_id="delete_batch_object", region=ALICLOUD_REGION, bucket_name=BUCKET_NAME, keys=[OBJECT_KEY], file_path=OBJECT_FILE_PATH)

    oss_create_bucket >> oss_delete_bucket
    oss_create_bucket >> upload_object
    download_object >> delete_object
