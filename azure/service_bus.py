from __future__ import annotations
import os
from datetime import datetime, timedelta
import pytest
from airflow.models.dag import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

try:
    from airflow.providers.microsoft.azure.operators.asb import (
        ASBReceiveSubscriptionMessageOperator,
        AzureServiceBusCreateQueueOperator,
        AzureServiceBusDeleteQueueOperator,
        AzureServiceBusReceiveMessageOperator,
        AzureServiceBusSendMessageOperator,
        AzureServiceBusSubscriptionCreateOperator,
        AzureServiceBusSubscriptionDeleteOperator,
        AzureServiceBusTopicCreateOperator,
        AzureServiceBusTopicDeleteOperator,
        AzureServiceBusUpdateSubscriptionOperator,
    )
except ImportError:
    pytest.skip("Azure Service Bus not available", allow_module_level=True)

DAG_ID = "servicebus"
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
CLIENT_ID = os.environ.get("CLIENT_ID")
QUEUE_NAME = os.environ.get("QUEUE_NAME")
MESSAGE = os.environ.get("MESSAGE")
MESSAGE_LIST = [f"{MESSAGE} {n}" for n in range(10)]
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SUBSCRIPTION_NAME = os.environ.get("SUBSCRIPTION_NAME")
AZURE_SERVICE_BUS_CONNECTION_ID = os.environ.get("AZURE_SERVICE_BUS_CONNECTION_ID")
default_args = {
        "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
        "azure_service_bus_conn_id": AZURE_SERVICE_BUS_CONNECTION_ID,
    }
tags = ["example","service_bus"]

with DAG(DAG_ID, start_date=datetime(2021, 8, 13), schedule=None, catchup=False,
         default_args=default_args, tags=tags) as dag:
    create_queue = AzureServiceBusCreateQueueOperator(
        task_id="create_service_bus_queue",
        queue_name=QUEUE_NAME,
    )

    send_to_queue = AzureServiceBusSendMessageOperator(
        task_id="send_message_to_service_bus_queue",
        message=MESSAGE,
        queue_name=QUEUE_NAME,
        batch=False,
    )

    send_list_to_queue = AzureServiceBusSendMessageOperator(
        task_id="send_list_message_to_service_bus_queue",
        message=MESSAGE_LIST,
        queue_name=QUEUE_NAME,
        batch=False,
    )

    send_batch_to_queue = AzureServiceBusSendMessageOperator(
        task_id="send_batch_message_to_service_bus_queue",
        message=MESSAGE_LIST,
        queue_name=QUEUE_NAME,
        batch=True,
    )

    recieve_message = AzureServiceBusReceiveMessageOperator(
        task_id="receive_message_service_bus_queue",
        queue_name=QUEUE_NAME,
        max_message_count=20,
        max_wait_time=5,
    )

    create_topic = AzureServiceBusTopicCreateOperator(
        task_id="create_service_bus_topic", topic_name=TOPIC_NAME
    )

    delete_topic = AzureServiceBusTopicDeleteOperator(
        task_id="delete_asb_topic",
        topic_name=TOPIC_NAME,
    )

    create_subscription = AzureServiceBusSubscriptionCreateOperator(
        task_id="create_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
    )

    update_subscription = AzureServiceBusUpdateSubscriptionOperator(
        task_id="update_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        max_delivery_count=5,
    )

    receive_message_subscription = ASBReceiveSubscriptionMessageOperator(
        task_id="receive_message_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        max_message_count=10,
    )

    delete_sb_subscription = AzureServiceBusSubscriptionDeleteOperator(
        task_id="delete_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        trigger_rule="all_done",
    )

    delete_sb_queue = AzureServiceBusDeleteQueueOperator(
        task_id="delete_service_bus_queue", queue_name=QUEUE_NAME, trigger_rule="all_done"
    )
    chain(
        create_queue,
        create_topic,
        create_subscription,
        send_to_queue,
        send_list_to_queue,
        send_batch_to_queue,
        recieve_message,
        update_subscription,
        receive_message_subscription,
        delete_sb_subscription,
        delete_topic,
        delete_sb_queue
    )