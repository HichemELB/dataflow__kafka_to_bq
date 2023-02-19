import os
import pytest
import tempfile
from kafka import KafkaProducer
from google.cloud import bigquery

from kafka_to_bigquery import KafkaToBigQuery


@pytest.fixture(scope="module")
def test_data():
    return [
        "1645065600,a,b,c",
        "1645069200,d,e,f",
        "1645072800,g,h,i"
    ]


@pytest.fixture(scope="module")
def pipeline_options():
    return {
        "options": {
            "project": os.environ["BIGQUERY_PROJECT"],
            "runner": "DirectRunner",
            "streaming": False,
            "temp_location": tempfile.mkdtemp(),
            "region": "us-central1",
            "service_account_email": os.environ["GCP_SERVICE_ACCOUNT"],
            "save_main_session": True
        },
        "google_cloud_options": {
            "project": os.environ["BIGQUERY_PROJECT"],
            "table": f"{os.environ['BIGQUERY_DATASET']}.{os.environ['BIGQUERY_TABLE']}"
        },
        "kafka": {
            "brokers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "consumer_group": os.environ["KAFKA_CONSUMER_GROUP"]
        },
        "batch_inserts": {
            "max_buffer_size": 1000,
            "schema": [
                {"name": "publish_at", "type": "TIMESTAMP"},
                {"name": "load_at", "type": "TIMESTAMP"},
                {"name": "record", "type": "STRING"}
            ]
        }
    }


@pytest.fixture(scope="module")
def topic():
    return os.environ["KAFKA_TOPICS"].split(",")[0]


@pytest.fixture(scope="module")
def gcp_client():
    return bigquery.Client()


def test_kafka_to_bigquery_pipeline(test_data, pipeline_options, topic, gcp_client):
    # Write the test data to Kafka
    for data in test_data:
        producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        producer.send(topic, value=data.encode("utf-8"))
        producer.flush()

    # Run the pipeline
    KafkaToBigQuery().run_pipeline(
        pipeline_options=pipeline_options,
        kafka_config_file="kafka_config.yml",
        gcp_config_file="gcp_config.yml"
    )

    # Verify the data in BigQuery
    table_id = f"{os.environ['BIGQUERY_PROJECT']}.{os.environ['BIGQUERY_DATASET']}.{os.environ['BIGQUERY_TABLE']}"
    rows = list(gcp_client.query(f"SELECT * FROM {table_id}").result())

    assert len(rows) == len(test_data)
    assert rows[0]["record"] == test_data[0].split(",")[-1]
    assert rows[1]["record"] == test_data[1].split(",")[-1]
    assert rows[2]["record"] == test_data[2].split(",")[-1]
