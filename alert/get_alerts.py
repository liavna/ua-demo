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

hello_task = PythonOperator(
    task_id='hello_task',  # Task ID
    python_callable=print_hello,  # Function to be called
    dag=dag,  # Assign the DAG to the task
)

# Set task dependencies (if you have more tasks in the DAG)
# e.g., hello_task.set_upstream(another_task)

if __name__ == "__main__":
    dag.cli()
