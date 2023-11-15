from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_save_json',
    default_args=default_args,
    description='Fetch and save JSON from URL',
    schedule_interval='@daily',
    access_control={
		'role_liav': {
			'can_read',
			'can_edit',
			'can_delete'
		},
        'role_Admin': {
			'can_read',
			'can_edit',
			'can_delete'
		}
	},
# Adjust as needed
)

def fetch_and_save_json(**kwargs):
    url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Save data to working folder
        output_path = 'local:///tmp/' , 'output.json')
        with open(output_path, 'w') as file:
            json.dump(data, file)
    else:
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")

# Define the tasks
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_task',
    python_callable=fetch_and_save_json,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_and_save_task

if __name__ == "__main__":
    dag.cli()
