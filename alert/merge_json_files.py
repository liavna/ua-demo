from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import json
import os

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'merge_json_files',
    default_args=default_args,
    description='DAG to merge two JSON files',
    schedule_interval=None,  # Set to your desired schedule or None if you want to trigger manually
)

# Function to merge JSON files
def merge_json_files(**kwargs):
    source_path = "/mounts/shared-volume/json"
    
    # List files in the source directory
    files = os.listdir(source_path)
    
    if len(files) != 2:
        raise ValueError("Exactly two JSON files are required for merging.")
    
    # Load JSON data from files
    file1_path = os.path.join(source_path, files[0])
    file2_path = os.path.join(source_path, files[1])
    
    with open(file1_path, 'r') as file1:
        data1 = json.load(file1)
    
    with open(file2_path, 'r') as file2:
        data2 = json.load(file2)
    
    # Merge JSON data
    merged_data = {**data1, **data2}
    
    # Save the merged data to a new file
    output_path = os.path.join(source_path, 'merged_output.json')
    with open(output_path, 'w') as output_file:
        json.dump(merged_data, output_file)
    
    return output_path

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)

merge_json_task = PythonOperator(
    task_id='merge_json',
    python_callable=merge_json_files,
    provide_context=True,
    dag=dag,
)

# Set up dependencies
start_task >> merge_json_task
