from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the default_args
dag = DAG(
    'merge_json_files',
    default_args=default_args,
    description='A DAG to merge two JSON files',
    schedule_interval=timedelta(days=1),  # Adjust the schedule_interval as needed
)

# Define the script to merge JSON files
merge_script = """
python3 - <<EOF
import json

# Paths to the JSON files
file1_path = '/mounts/shared-volume/json/file1.json'
file2_path = '/mounts/shared-volume/json/file2.json'
output_path = '/mounts/shared-volume/json/merged.json'

# Read JSON files
with open(file1_path, 'r') as file1:
    data1 = json.load(file1)

with open(file2_path, 'r') as file2:
    data2 = json.load(file2)

# Merge the data
merged_data = data1 + data2

# Write the merged data to the output file
with open(output_path, 'w') as output_file:
    json.dump(merged_data, output_file)
EOF
"""

# Use BashOperator to execute the merge script
merge_task = BashOperator(
    task_id='merge_json_files',
    bash_command=merge_script,
    dag=dag,
)

# Set task dependencies
merge_task

if __name__ == "__main__":
    dag.cli()
