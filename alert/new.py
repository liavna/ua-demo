from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago


def get_alerts_new(**kwargs):
    url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    save_path = "/usr/local/airflow/dags/AlertsHistory.json"

    response = requests.get(url)
    data = response.json()

    with open(save_path, 'w') as json_file:
        json.dump(data, json_file)

    print(f"Downloaded and saved JSON data to {save_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
	}
)

download_task = PythonOperator(
    task_id='download_json_task',
    python_callable=get_alerts_new,
    provide_context=True,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
