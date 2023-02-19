import logging
from datetime import datetime
from typing import Tuple, List
import pytz
import yaml
import fire

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms import window
from apache_beam.transforms.window import TimestampedValue
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


class KafkaToBigQuery:
    """A class that reads from Kafka and writes to BigQuery using Apache Beam.

    Attributes:
        None
    """

    def run_pipeline(self, pipeline_options_file: str, kafka_config_file: str, gcp_config_file: str, **kwargs):
        """Runs the pipeline.

        Args:
            pipeline_options_file (str): Path to the YAML file with pipeline options.
            kafka_config_file (str): Path to the YAML file with Kafka configurations.
            gcp_config_file (str): Path to the YAML file with GCP configurations.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        # Load the pipeline options from the YAML config file
        with open(pipeline_options_file, "r") as f:
            pipeline_options = yaml.safe_load(f)

        # Load the Kafka config from the YAML config file
        with open(kafka_config_file, "r") as f:
            kafka_config = yaml.safe_load(f)

        # Load the GCP config from the YAML config file
        with open(gcp_config_file, "r") as f:
            gcp_config = yaml.safe_load(f)

        # Create the PipelineOptions object from the dictionary
        options = PipelineOptions.from_dictionary(pipeline_options["options"])
        google_cloud_options = options.view_as(GoogleCloudOptions)

        # Define the pipeline
        with beam.Pipeline(options=options) as p:
            # Create a PCollection of Kafka topic names
            topics = p | "Create topic list" >> beam.Create(kafka_config["kafka_topics"])

            # Read from each Kafka topic in parallel
            kafka_data = (topics
                          | "Read from Kafka" >> beam.ParDo(ReadFromKafka(
                    consumer_config={
                        "bootstrap.servers": pipeline_options["kafka"]["brokers"],
                        "group.id": pipeline_options["kafka"]["consumer_group"]
                    },
                    topics=[beam.DoFn.Input()],
                    key_deserializer=lambda x: x.decode("utf-8"),
                    value_deserializer=lambda x: x.decode("utf-8")
                ))
                          | "Decode message" >> beam.Map(lambda x: x.value)
                          | "Add timestamps" >> beam.Map(lambda x: TimestampedValue(x, int(x.split(',')[0])))
                          | "Window into daily" >> beam.WindowInto(window.FixedWindows(24 * 60 * 60))
                          )

            # Transform data
            transformed_data = kafka_data | "Extract fields" >> beam.Map(
                lambda x: {
                    "publish_at": x.split(',')[0],
                    "load_at": datetime.now(pytz.utc),
                    "record": x.split(',')[3]
                }
            )

            # Write to BigQuery
            transformed_data | "Write to BigQuery" >> beam.ParDo(
                BigQuerySink(
                    pipeline_options["google_cloud_options"]["table"],
                    pipeline_options["batch_inserts"]["max_buffer_size"],
                    pipeline_options["batch_inserts"]["schema"]
                )
            )


class BigQuerySink(beam.DoFn):
    """
    A DoFn that writes to BigQuery using the specified batch size.

    This DoFn buffers data until a maximum buffer size is reached, and then writes
    the buffered data to a BigQuery table using a batch insert. The table is created
    with the specified schema if it does not already exist.

    Args:
        config (dict): The dictionary that holds the configurations for the class.

    Attributes:
        buffer (List): The buffer that holds the buffered records.
    """

    def __init__(self, config: dict):
        self.config = config
        self.buffer = []

    def start_bundle(self) -> None:
        self.buffer = []

    def process(self, element: Tuple[str, datetime, str], timestamp=beam.DoFn.TimestampParam) -> None:
        self.buffer.append(element)

        if len(self.buffer) >= self.config['batch_inserts']['max_buffer_size']:
            self.write_batch()

    def finish_bundle(self) -> None:
        if self.buffer:
            self.write_batch()

    def write_batch(self) -> None:
        today = datetime.utcnow().date().strftime("%Y%m%d")

        # Create the table name with the topic and date suffix
        table_name_with_suffix = f"{self.config['google_cloud_options']['table']}_{self.config['kafka']['topic']}_{today}"
        table_spec = f"{self.config['google_cloud_options']['project']}:{self.config['google_cloud_options']['dataset']}.{table_name_with_suffix}"

        # Check if the table exists, and create it with the schema if it does not exist
        client = bigquery.Client()
        try:
            client.get_table(table_spec)
        except bigquery.NotFound:
            # Table does not exist, create it with the schema
            schema_fields = [SchemaField(field['name'], field['type']) for field in self.config['batch_inserts']['schema']]
            schema = bigquery.Schema(schema_fields)
            table = bigquery.Table(table_spec, schema=schema)
            table = client.create_table(table)
            logging.info(f"Table {table_spec} created successfully")
        else:
            logging.info(f"Table {table_spec} already exists")

        # Write the data to the table using the BigQuery sink
        with beam.io.gcp.bigquery.BigQuerySink(
                table_spec,
                schema=self.config['batch_inserts']['schema'],
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                batch_size=self.config['batch_inserts']['max_buffer_size']
        ).open() as sink:
            for row in self.buffer:
                sink.write(row)

        # Clear the buffer
        self.buffer = []


if __name__ == "__main__":
    fire.Fire(KafkaToBigQuery().run_pipeline)
