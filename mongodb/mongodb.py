from __future__ import annotations
import pytest, os
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MONGODB_HOST = os.environ.get("MONGODB_HOST")
MONGODB_PORT = os.environ.get("MONGODB_PORT")
MONGODB_SCHEMA = os.environ.get("MONGODB_SCHEMA")


@pytest.fixture(scope="module", autouse=True)
def mongo_connections():
    connections = [
        Connection(conn_id="mongo_default", conn_type="mongo", host=MONGODB_HOST, port=MONGODB_PORT),
        Connection(conn_id="mongo_test", conn_type="mongo", host=MONGODB_HOST, port=MONGODB_PORT, schema=MONGODB_SCHEMA),
    ]

    with pytest.MonkeyPatch.context() as mp:
        for conn in connections:
            mp.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.as_json())
        yield


@pytest.mark.integration("mongo")
class TestMongoSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)

        hook = MongoHook(mongo_conn_id="mongo_test")
        hook.insert_one("foo", {"bar": "baz"})

        self.sensor = MongoSensor(
            task_id="test_task",
            mongo_conn_id="mongo_test",
            dag=self.dag,
            collection="foo",
            query={"bar": "baz"},
        )

    def test_poke(self):
        assert self.sensor.poke(None)

    def test_sensor_with_db(self):
        hook = MongoHook(mongo_conn_id="mongo_test")
        hook.insert_one("nontest", {"1": "2"}, mongo_db="nontest")

        sensor = MongoSensor(
            task_id="test_task2",
            mongo_conn_id="mongo_test",
            dag=self.dag,
            collection="nontest",
            query={"1": "2"},
            mongo_db="nontest",
        )
        assert sensor.poke(None)