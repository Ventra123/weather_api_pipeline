# Weather Data Pipeline: OpenWeatherMap API to GCS to BigQuery

## Overview

This data pipeline, orchestrated by GCP's Cloud Composer, is designed to ingest data from an API, store the ingested data as a new JSON file in Google Cloud Storage (GCS), and then read the latest file from GCS to append the data to a table in BigQuery. The pipeline ensures the seamless flow of data from the source API to a structured format in BigQuery for further analysis and reporting.

## Data Flow

- Cloud Composer schedules and triggers the API ingestion task at regular intervals, based on the specified schedule.
- The API ingestion task, implemented as a Python script, makes API requests to OpenWeatherMap fetch the latest weather data (limited to three cities for the purpose of this project), processes it, and transforms it into JSON format.
- The JSON data is then written as a new file in the GCS bucket, ensuring data persistence and availability.
- Cloud Composer triggers the data loading task, which reads the latest file from GCS and loads the data into the designated BigQuery table every hour. Schema mapping is applied to ensure compatibility and data consistency.
- The ingested data is now available in BigQuery, where it can be queried, analyzed, and utilized for reporting purposes.

## Data Sources and Schemas
The 'weather_schema.json' file maps the schema for the BigQuery table

## Data Transformations
Light touch data transformation is performed when ingesting the data from the API call to convert the JSON file to NDJSON file format as per BigQuery's table load requirements. Date partitioning is applied before loading into BigQuery

## Not in Scope
- Testing and Validation
- Error Handling and Monitoring
