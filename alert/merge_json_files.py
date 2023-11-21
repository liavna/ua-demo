from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'merge_json_files',
    default_args=default_args,
    description='Merge two JSON files',
    schedule_interval='@daily',  # Adjust the schedule as needed
)

def merge_json_files(**kwargs):
    # Get the list of JSON files in the specified path
    folder_path = "file:///mounts/shared-volume/json"
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    # Check if there are at least two files to merge
    if len(json_files) < 2:
        print("Not enough JSON files to merge.")
        return

    # Assuming the file names contain unique identifiers
    file1, file2 = json_files[:2]

    # Load JSON data from the files
    df1 = pd.read_json(f"{folder_path}/{file1}")
    df2 = pd.read_json(f"{folder_path}/{file2}")

    # Merge the DataFrames based on common columns
    merged_df = pd.merge(df1, df2, on="common_column", how="inner")

    # Save the merged DataFrame to a new JSON file
    output_path = f"{folder_path}/merged_output.json"
    merged_df.to_json(output_path, orient="records")

    print(f"Merged data saved to {output_path}")

# Define the tasks in the DAG
merge_json_task = PythonOperator(
    task_id='merge_json',
    python_callable=merge_json_files,
    provide_context=True,  # Pass context (e.g., execution_date) to the Python function
    dag=dag,
)

# Set task dependencies
merge_json_task

if __name__ == "__main__":
    dag.cli()
