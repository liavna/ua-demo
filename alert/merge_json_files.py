from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.security import permissions

def run_spark_job():
    # Your PySpark code goes here
    # ...

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
    'json_to_csv_spark',
    default_args=default_args,
    description='Convert JSON to CSV using PySpark',
    schedule_interval=None,  # You can set a specific schedule if needed
    access_control={
        'role_liav': {
            permissions.ACTION_CAN_READ,
            permissions.ACTION_CAN_EDIT,
            permissions.ACTION_CAN_DELETE,
        },
        'role_Admin': {
            permissions.ACTION_CAN_READ,
            permissions.ACTION_CAN_EDIT,
            permissions.ACTION_CAN_DELETE,
        },
    },
)

run_spark_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag,
    # Set access_control for this specific task
    access_control={'role_liav', 'role_Admin'},
)

# Define the task dependencies
run_spark_task  # Make sure to use `>>` or `set_downstream` to define task dependencies

if __name__ == "__main__":
    dag.cli()
