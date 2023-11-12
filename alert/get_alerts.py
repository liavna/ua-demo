from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'json_file_from_url',
    default_args=default_args,
    description='A DAG to read JSON file from URL',
    schedule_interval=timedelta(days=1),
)

# Define the URL of the JSON file
json_url = 'https://example.com/path/to/your/json/file.json'

# Use HttpSensor to wait until the file is available
wait_for_file = HttpSensor(
    task_id='wait_for_file',
    method='HEAD',
    http_conn_id='http_default',  # You may need to define an HTTP connection in Airflow
    endpoint=json_url,
    poke_interval=5,
    timeout=20,
    dag=dag,
)

# Use SimpleHttpOperator to fetch the JSON file
fetch_file = SimpleHttpOperator(
    task_id='fetch_file',
    method='GET',
    http_conn_id='http_default',
    endpoint=json_url,
    xcom_push=True,
    dag=dag,
)

# Define a Python function to process the JSON data
def process_json_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_file')
    # Process the JSON data as needed
    print(json_data)

# Use a PythonOperator to process the JSON data
process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_json_data,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
wait_for_file >> fetch_file >> process_data
