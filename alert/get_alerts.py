from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
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

def fetch_and_save_to_database(**kwargs):
    url = "https://www.mako.co.il/Collab/amudanan/alerts.json"
    response = requests.get(url)
    data = response.json()

    # Assuming a PostgreSQL database connection
    pg_hook = PostgresHook(postgres_conn_id='presto://ezpresto-svc-https-locator.ezpresto:8081/cache')
    connection = pg_hook.get_conn()

    # Example: Assuming there is a table called 'your_table' with columns 'column1', 'column2'
    cursor = connection.cursor()
    for record in data:
        cursor.execute(
            "INSERT INTO your_table (column1, column2) VALUES (%s, %s)",
            (record['field1'], record['field2'])
        )
    connection.commit()
    cursor.close()
    connection.close()

# Define the tasks in the DAG
fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_to_database',
    python_callable=fetch_and_save_to_database,
    provide_context=True,
    dag=dag,
)

# Set task dependencies if necessary
# task2.set_upstream(task1)

if __name__ == "__main__":
    dag.cli()
