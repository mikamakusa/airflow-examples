from __future__ import annotations
import os
from datetime import datetime
from requests import Request
from airflow import DAG
from airflow.decorators import task
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

JENKINS_CONNECTION_ID = os.environ.get("JENKINS_CONNECTION_ID")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
ARTIFACT_URL = os.environ.get("ARTIFACT_URL")
DAG_ID = "jenkins"

default_args={
        "retries": 1,
        "concurrency": 8,
        "max_active_runs": 8,
    }

with DAG(DAG_ID, default_args, start_date=datetime(2017, 6, 1), schedule="@once") as dag:
    job_trigger = JenkinsJobTriggerOperator(
        task_id="trigger_job",
        job_name="generate-merlin-config",
        parameters={"first_parameter": "a_value", "second_parameter": "18"},
        jenkins_connection_id=JENKINS_CONNECTION_ID
    )

    @task
    def grab_artifact_from_jenkins(url):
        hook = JenkinsHook(JENKINS_CONNECTION_ID)
        jenkins_server = hook.get_jenkins_server()
        url += "artifact/myartifact.xml"
        request = Request(method="GET", url=url)
        response = jenkins_server.jenkins_open(request)
        return response

    grab_artifact_from_jenkins(job_trigger.output)