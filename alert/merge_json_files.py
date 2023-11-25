from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import requests
import json
import os
import shutil
import glob

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'json_to_csv_dag',
    default_args=default_args,
    description='Convert JSON to CSV with PySpark and Airflow',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
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

# Define the Python function to convert JSON to CSV
def convert_json_to_csv():
    # Set up Spark session
    spark = SparkSession.builder.appName("JsonToCsv").getOrCreate()

    # JSON URL
    json_url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"

    # Fetch JSON data
    response = requests.get(json_url)
    data_json = response.json()

    # Convert JSON data to a JSON string
    data_json_str = json.dumps(data_json)

    # Create a PySpark DataFrame from the JSON data
    df = spark.read.json(spark.sparkContext.parallelize([data_json_str]))

    # Specify the CSV file path
    csv_file_path = "file:///mounts/shared-volume/ua-demo/alerts/data/"

    # Repartition to a single partition and coalesce to a single file
    df.repartition(1).coalesce(1).write.csv(
        csv_file_path,
        header=True,
        mode="overwrite",
        compression="none"
    )

    # Delete _SUCCESS file and part-00000* files
    success_file = os.path.join(csv_file_path, "_SUCCESS")

    if os.path.exists(success_file):
        os.remove(success_file)

    # Use shutil to remove multiple files matching the pattern
    for file in glob.glob(os.path.join(csv_file_path, "*.csv")):
        os.remove(file)

    # Stop the Spark session
    spark.stop()

# Define the PythonOperator to run the conversion task
convert_task = PythonOperator(
    task_id='convert_json_to_csv',
    python_callable=convert_json_to_csv,
    dag=dag,
)

# Set task dependencies if needed
# convert_task.set_upstream(...)

# You can continue adding more tasks to the DAG as needed

if __name__ == "__main__":
    dag.cli()
