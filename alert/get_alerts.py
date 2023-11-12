from datetime import datetime, timedelta
from urllib import request
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'your_username',
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
		'All': {
			'can_read',
			'can_edit',
			'can_delete'
		}
	},
)

def read_json_from_url(**kwargs):
    url = "https://www.mako.co.il/Collab/amudanan/alerts.json"
    response = request.urlopen(url)
    data = json.load(response)
    # Do something with the data, for example, print it
    print(data)

task_read_json = PythonOperator(
    task_id='read_json',
    python_callable=read_json_from_url,
    provide_context=True,  # This allows you to access the context, including the execution date
    dag=dag,
)

task_read_json
