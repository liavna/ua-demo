from datetime import datetime, timedelta
from airflow import DAG
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}

dag = DAG(
    'https_json_dag',
    default_args=default_args,
    description='A simple DAG to fetch JSON data from an HTTPS site',
    schedule_interval=timedelta(days=1),  # Set your desired schedule
)

# Use HttpSensor to wait until the site is available
wait_for_site = HttpSensor(
    task_id='wait_for_site',
    method='HEAD',
    http_conn_id='http_default',
    endpoint='/path/to/your/site',
    poke_interval=5,  # Check every 5 seconds
    timeout=20,  # Timeout after 20 seconds
    dag=dag,
)

# Use HttpOperator to get JSON data from the site
get_json_data = SimpleHttpOperator(
    task_id='get_json_data',
    method='GET',
    http_conn_id='http_default',
    endpoint='/path/to/your/json/endpoint',
    response_check=lambda response: True if response.json() else False,
    dag=dag,
)

# Use PythonOperator to process the JSON data
def process_json(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='get_json_data')
    json_data = response.json()
    # Add your processing logic here
    print(json_data)

process_json_task = PythonOperator(
    task_id='process_json',
    python_callable=process_json,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
wait_for_site >> get_json_data >> process_json_task
