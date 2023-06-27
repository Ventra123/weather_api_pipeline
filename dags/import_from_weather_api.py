
"""
Script to get data from OpenWeatherMap API for three cities. More cities found in city.list.json
"""

import requests
import json
from google.cloud import storage
import datetime

def import_weather_data():
    api_key = "94d53f01db456ce547995b0116262c5f"
    city_ids = [("Los Angeles", 5368361), ("Seattle", 5809844), ("New York", 5128581)]
    get_weather_data(api_key, city_ids)

def get_weather_data(api_key, city_ids):
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    session = requests.Session()
    file_prefix = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f"{file_prefix}__data.ndjson"

    def weather_data_generator(city_ids):
        for i, (city_name, city_id) in enumerate(city_ids):
            complete_url = f"{base_url}appid={api_key}&id={city_id}"
            response = session.get(complete_url)
            if response.ok:
                response_data = response.json()
            else:
                continue
            if response_data:
                yield response_data

    ndjson = ""
    for data in weather_data_generator(city_ids):
        data['dt'] = datetime.datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        ndjson += json.dumps(data) + "\n"
    
    write_file_to_gcs(file_name, ndjson)

def write_file_to_gcs(file_name, ndjson):
    try:
        bucket_name = 'us-west1-etl-composer-5680954a-bucket'
        folder_path = 'daily_load_weather_data' + '/'
        destination_blob_name = folder_path + file_name

        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(ndjson)
    except Exception as e:
        logging.error(e)



 

