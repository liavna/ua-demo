from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.operators.python_operator import PythonOperator
import json
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'json_file_from_url',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Define the URL of the JSON file
json_file_url = 'https://example.com/data.json'

# Define a task to wait for the JSON file to be available
wait_for_file = HttpSensor(
    task_id='wait_for_file',
    http_conn_id='http_default',  # Default connection ID
    endpoint='',  # Leave it empty as we are using the full URL
    request_params={},  # Additional request parameters if needed
    mode='poke',  # Use poke mode to repeatedly check the file's availability
    timeout=600,  # Timeout in seconds
    poke_interval=60,  # Interval between pokes in seconds
    dag=dag,
)

# Define a task to download the JSON file
download_file = SimpleHttpOperator(
    task_id='download_file',
    http_conn_id='http_default',  # Default connection ID
    method='GET',
    endpoint=json_file_url,  # Use the full URL as the endpoint
    headers={},  # Additional headers if needed
    response_filter=lambda response: response.json(),  # Parse the JSON response
    xcom_push=True,  # Push the downloaded data to XCom for the next task
    dag=dag,
)

# Define a task to process the JSON data
def process_json_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='download_file')
    
    # Process the JSON data as needed
    # For example, print the data
    print(json_data)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_json_data,
    provide_context=True,
    dag=dag,
)

# Set up the task dependencies
wait_for_file >> download_file >> process_data
