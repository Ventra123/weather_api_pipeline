
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json
import re


#TODO: it makes more sense to group the two functions within a class so they can all share the same variables without duplicating efforts
def load_table(bucket_name, file_name, project_id, dataset_id, table_id, partition_field, write_disposition, schema_file):


    # Initialize the BigQuery and Storage clients
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()

    # Set up the GCS file path
    gcs_uri = f"gs://{bucket_name}/{file_name}"

    # Set up the BigQuery dataset and table references
    dataset_ref = bigquery_client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Create a table if it doesn't exist
    if partition_field:
        create_table_if_not_exists(bigquery_client, storage_client, project_id, dataset_id, table_id, partition_field,bucket_name, schema_file)

    # Create the BigQuery job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    #job_config.skip_leading_rows = 1
    job_config.autodetect = True

    # Set write disposition if provided
    if write_disposition:
        job_config.write_disposition = write_disposition

    # Start the BigQuery load job
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )

    # Wait for the load job to complete
    load_job.result()

    # Check if the load job was successful
    if load_job.state == "DONE":
        print(f"Data {write_disposition}ed successfully from GCS to BigQuery.")
    else:
        print(f"Data {write_disposition} job failed.")

    # Check if the schema has changed and update it accordingly
    # TODO: add a behavior to raise an error that the schema has changed and stop the process from continuing
    table = bigquery_client.get_table(table_ref)
    if table.schema != load_job.schema:
        table.schema = load_job.schema
        table = bigquery_client.update_table(table, ["schema"])
        print(f"Schema updated successfully.")


def create_table_if_not_exists(bigquery_client, storage_client, project_id, dataset_id, table_id, partition_field,bucket_name,schema_file):


    # Set up theQuery dataset and table references
    dataset_ref = bigquery_client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Attempt to get the table, creating it if it does not exist
    try:
        bigquery_client.get_table(table_ref)
        print("Table already exists.")
    except NotFound:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(schema_file)
        file_contents = blob.download_as_bytes() # TODO: you can delete this as it's not used
        schema = json.loads(blob.download_as_string(client=None))
        table_prefix = f"{project_id}.{dataset_id}"
        table = bigquery.Table(f"{table_prefix}.{table_id}")
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        table.schema = schema
        table = bigquery_client.create_table(table)
        print("Created table")
    except Exception as e:
        raise e


# TODO: you can delete this
# bucket_name = "us-west1-etl-composer-5680954a-bucket"
# file_name = "daily_load_weather_data/20230624-174149__data.ndjson"
# project_id = "dwh-weather-api"
# dataset_id = "raw"
# table_id = "raw_weather_load"
# partition_field = "dt" # Replace with your partitioning field name
# write_disposition = "WRITE_APPEND"
# schema_file = "dags/weather_schema.json" # Replace with schema file path


# load_table(bucket_name, file_name, project_id, dataset_id, table_id, partition_field, write_disposition, schema_file)
