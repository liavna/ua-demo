from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor

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
    'json_download_dag',
    default_args=default_args,
    description='A simple DAG to retrieve a JSON file from a URL',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
)

# Sensor to wait for the URL to become available
wait_for_url_sensor = HttpSensor(
    task_id='wait_for_url_sensor',
    http_conn_id='http_default',  # Set your HTTP connection ID
    endpoint='/path/to/your/url',
    timeout=600,  # Set an appropriate timeout value
    poke_interval=60,  # Set an appropriate interval for poking
    dag=dag,
)

# Operator to download the JSON file
download_json_operator = SimpleHttpOperator(
    task_id='download_json_operator',
    http_conn_id='http_default',  # Set your HTTP connection ID
    endpoint='/path/to/your/jsonfile.json',
    method='GET',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    xcom_push=True,  # Push the response to XCom for potential downstream tasks
    dag=dag,
)

# Set the task dependencies
wait_for_url_sensor >> download_json_operator

if __name__ == "__main__":
    dag.cli()
