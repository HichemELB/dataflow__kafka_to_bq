# Kafka to BigQuery Pipeline

A pipeline that reads from Kafka and writes to BigQuery using Apache Beam.

## Table of Contents
1. [Introduction](#introduction)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Configuration](#Configuration)
6. [Known issues](#Known_issues)
7. [Todo](#todo)
8. [License](#License)

## Introduction

This is a pipeline that reads from Kafka and writes to BigQuery using Dataflow.
It is built using Python, and uses the `dataflow` and `google-cloud-bigquery` libraries. 
The pipeline has the following steps:

1. Read from Kafka.
2. Decode the message.
3. Add a timestamp to the message.
4. Window the data into daily intervals.
5. Transform the data.
6. Write the data to BigQuery.

The pipeline is designed to be configurable, so that it can be easily adapted to different use cases.

## Requirements

The pipeline requires the following:

- Python 3.8 or later
- A Kafka cluster
- A GCP project with BigQuery

## Installation

To install the pipeline, follow these steps:

1. Clone the repository.
2. Install the required packages by running the following command in the repository directory:
```shell
pip install -e .
```
3. Create a configuration file for the pipeline by copying the example file `pipeline.yml.example` and filling in the required information.
4. Create a configuration file for the Kafka topics by copying the example file `kafka.yml.example` and filling in the required information. 
5. Create a configuration file for the Google Cloud Platform by copying the example file `gcp.yml.example` and filling in the required information.

## Usage

The pipeline requires three YML configuration files: 
- The pipeline options YML file
- The Kafka topics Yml file
- The Google Cloud Platform config file.

These files are located in the config directory, and they should be edited to match your environment.

To run the pipeline, use the following command:
```shell
python kafka_to_bq.py run_pipeline \
      --pipeline-options /path/to/config/pipeline.yml \
      --kafka-topics /path/to/config/kafka.yml \
      --gcp_config /path/to/config/gcp.yml
```
Replace `/path/to/config/pipeline.yml`, `/path/to/config/kafka.yml` & `/path/to/config/gcp.yml` with the paths to 
the pipeline options, Kafka topics files & Google Cloud config, respectively.

You can add additional command-line arguments as needed, and they will be passed to the `run_pipeline()`function as keyword arguments.

## Configuration
The configuration files for the pipeline are located in the config directory. There are three files: pipeline.yml, kafka.yml & gcp.yml

1. `pipeline.yml`
This file contains two sections.
- options which are the options for the Dataflow pipeline itself. The available options are:
  - `runner`: The Beam runner to use (e.g. DirectRunner, DataflowRunner). 
  - `project`: The Google Cloud project to use. 
  - `temp_location`: The GCS location to use for temporary files (e.g. gs://bucket-name/tmp). 
  - `region`: The region to use for the Dataflow runner (if applicable). 
  - `job_name`: The name to use for the Dataflow job (if applicable).
- batch_inserts, this section contains the options for the batch inserts to BigQuery. The available options are:
  - `max_buffer_size`: The maximum number of records to buffer before inserting into BigQuery.
  - `schema`: The schema of the BigQuery table. This should be a list of dictionaries, where each dictionary has name and type keys.

2. `gcp.yml`
This file contains the options for the Google Cloud services used by the pipeline. The available options are:
   - `table`: The name of the BigQuery table to write to. 
   - `dataset`: The name of the BigQuery dataset to use.

- `kafka.yml`
This file contains two sections:
- `kafka`which contains the options for the Kafka consumer. The available options are:
  - `brokers`: A comma-separated list of Kafka brokers. 
  - `consumer_group`: The name of the consumer group to use.
- `kafka_topics` which is a list of Kafka topics to read from. The format of the file is as follows:

## Known issues

- None

## Todo

- Add error handling and retries.
- Add unit tests.
- Add support for more Dataflow runners.
- Add support for Avro and other message formats.

***
## License

This program is licensed under the MIT License. See the [LICENSE](https://opensource.org/license/mit/) file for details.