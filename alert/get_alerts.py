from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json

# Define the default_args dictionary to specify the default parameters of the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with the specified default_args.
dag = DAG(
    'read_json_from_url',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Adjust the schedule_interval as needed.
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

# Define the function that will be executed by the PythonOperator.
def fetch_and_process_json(**kwargs):
    url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    
    try:
        # Fetch JSON data from the URL.
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses.

        # Parse the JSON data.
        json_data = response.json()

        # Do further processing with the JSON data as needed.
        # For example, print the content of the JSON data.
        print(json.dumps(json_data, indent=2))

    except Exception as e:
        # Handle exceptions (e.g., connection errors, JSON parsing errors).
        print(f"Error fetching or processing JSON: {str(e)}")

# Define the PythonOperator to execute the fetch_and_process_json function.
fetch_json_task = PythonOperator(
    task_id='fetch_json',
    python_callable=fetch_and_process_json,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies.
fetch_json_task

if __name__ == "__main__":
    dag.cli()
