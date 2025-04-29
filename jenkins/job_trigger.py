from __future__ import annotations
import os
from datetime import datetime
from requests import Request
from airflow.decorators import task, dag
from airflow import DAG
from airflow.decorators import task
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

JENKINS_CONNECTION_ID = os.environ.get("JENKINS_CONNECTION_ID")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
ARTIFACT_URL = os.environ.get("ARTIFACT_URL")
DAG_ID = "jenkins"

@task
def grab_artifact_from_jenkins(url):
    hook = JenkinsHook(JENKINS_CONNECTION_ID)
    jenkins_server = hook.get_jenkins_server()
    url += ARTIFACT_URL
    request = Request(method="GET", url=url)
    response = jenkins_server.jenkins_open(request)
    return response

@dag(default_args={
        "retries": 1,
        "concurrency": 8,
        "max_active_runs": 8,
    },
    start_date=datetime(2017, 6, 1),
    schedule=None)
def jenkins_job_trigger_dag():
    grab_artifact_from_jenkins()