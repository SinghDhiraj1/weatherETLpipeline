from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Latitude and Longitude for the desired location (Mumbai)
LATITUDE = '12.97'
LONGITUDE = '77.5946'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date':days_ago(1)
}

## DAG
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        """ Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from airflow connection
        http_hook=HttpHook(http_conn_id=API_CONN_ID, method='GET')
        
        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=19.0760&longitude=72.8777&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        ## Make the requrest via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data:{response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data. """
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : LATITUDE,
            'longitude' : LONGITUDE,
            'temperature' : current_weather['temperature'],
            'windspeed' : current_weather['windspeed'],
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Load tranfromed data into PostgreSQL. """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS weather_data (
                       latitude FLOAT,
                       longitude FLOAT,
                       temperature FLOAT,
                       windspeed FLOAT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       );
                       """)
        
        # Insert transformed data into the table
        cursor.execute("""
                       INSERT INTO weather_data (latitude, longitude, temperature, windspeed)
                       VALUES (%s, %s, %s, %s)
                       """, (
                           transformed_data['latitude'],
                           transformed_data['longitude'],
                           transformed_data['temperature'],
                           transformed_data['windspeed']
                       ))
        
        conn.commit()
        cursor.close()

        ## DAG workfloa - ETL pipeline

    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)

