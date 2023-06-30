import datetime
import os
import re
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from import_from_weather_api import import_weather_data
from load_to_bq import load_table

dag_name = "weather_etl_dag"
bucket_name = "us-west1-etl-composer-5680954a-bucket"
project_id = "dwh-weather-api"
dataset_id = "raw"
table_id = "raw_weather_load"
partition_field = "dt"
write_disposition = "WRITE_APPEND"
schema_file = "dags/weather_schema.json"
pattern = re.compile("^\d{8}-\d{6}__data.ndjson")

default_args = {
    "owner": "Weather API Starter Project",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": days_ago(1),
}

dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval="@hourly",
    description="Pulls from OpenWeatherMAP and Pushes to BQ",
    catchup=False,  # Disable catch-up behavior
)


def find_recent_file(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="daily_load_weather_data/")
    correct_blobs = []
    file_names = []
    for blob in blobs:
        if pattern.match(blob.name.split("/")[-1]):
            correct_blobs.append(blob)
            file_names.append(blob.name.split("/")[-1])
    correct_blobs.sort(key=lambda x: x.name.split("/")[-1])
    return file_names[-1]


file_name = "daily_load_weather_data/" + find_recent_file(bucket_name)

import_api = PythonOperator(
    task_id="import_from_api", python_callable=import_weather_data, dag=dag
)

load_bq = PythonOperator(
    task_id="load_into_bigquery",
    python_callable=load_table,
    op_kwargs={
        "bucket_name": bucket_name,
        "file_name": file_name,
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "partition_field": partition_field,
        "write_disposition": write_disposition,
        "schema_file": schema_file,
    },
    dag=dag,
)

import_api >> load_bq
