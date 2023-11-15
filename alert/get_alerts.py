from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_and_save_json(**kwargs):
    url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    save_path = "/usr/local/airflow/AlertsHistory.json"

    response = requests.get(url)
    data = response.json()

    with open(save_path, 'w') as json_file:
        json.dump(data, json_file)

    print(f"Downloaded and saved JSON data to {save_path}")

dag = DAG(
    'json_download_dag',
    default_args=default_args,
    description='DAG to download JSON from URL and save locally',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
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

)

download_task = PythonOperator(
    task_id='download_json_task',
    python_callable=download_and_save_json,
    provide_context=True,
    dag=dag,
)

# Set task dependencies if you have more tasks in the DAG
# task2.set_upstream(task1)

if __name__ == "__main__":
    dag.cli()
