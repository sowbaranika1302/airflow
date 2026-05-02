from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
# Automatically handles the creation of tasks and dependencies and closes the DAG context after the function is executed
with DAG(
    dag_id = "Nasa_Asteroid_ETL",
    start_date = datetime.utcnow(),
    schedule = "@daily",
    catchup = False
) as dag:
    # Step 1: Create a table in Postgres to store asteroid data
    @task
    def create_asteroid_table():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connection")
        #SQL Query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS asteroids(
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

 # https://api.nasa.gov/planetary/apod?api_key=Zj5o2ADCF2qlZp8kLp0lsxPCqraoyvkLhWsgObWT
        
    # Step 2: Extract NASA API data
    extract_apod = HttpOperator(
        task_id = "extract_apod",
        method = "GET",
        http_conn_id = "nasa_api", # Connection id defined in Airflow Connections
        endpoint = "planetary/apod", # API endpoint for Astronomy Picture of the Day
        data = {"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"}, # Pass the API key from Airflow Connections
        response_filter = lambda response: response.json() # Parse the JSON response
    )

    # Step 3: Transform the data to match the Postgres table schema
    @task
    def transform_apod_data(response):
        apod_data = {
            'name': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    # Step 4: Load the transformed data into Postgres
    @task
    def load_apod_data(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connection")
        insert_query = """
        INSERT INTO asteroids (name, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['name'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    # Step 5: Verify the data with DBViewer

    # Step 6: Define task dependencies
    create_asteroid_table() >> extract_apod
    print (extract_apod.output) # Print the output of the API call for debugging
    transformed_date = transform_apod_data(extract_apod.output) 
    load_apod_data(transformed_date)
    

