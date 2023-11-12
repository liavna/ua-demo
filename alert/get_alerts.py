from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'liav',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example Airflow DAG to read JSON file from URL',
    schedule_interval=timedelta(days=1),
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

def fetch_and_save_json(**kwargs):
    url = "https://www.mako.co.il/Collab/amudanan/alerts.json"
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()

        # Specify the output folder
        output_folder = kwargs['output_folder']
        os.makedirs(output_folder, exist_ok=True)

        # Save the JSON data to a file
        output_file_path = os.path.join(output_folder, 'output.json')
        with open(output_file_path, 'w') as output_file:
            json.dump(json_data, output_file, indent=2)

        return f"File saved at {output_file_path}"
    else:
        raise Exception(f"Failed to fetch JSON from {url}. Status code: {response.status_code}")

task = PythonOperator(
    task_id='fetch_and_save_json',
    python_callable=fetch_and_save_json,
    provide_context=True,
    op_kwargs={'output_folder': 'local:///mnt/shared/ua-demo/'},
    dag=dag,
)
