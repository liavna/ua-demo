from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import requests
import json
import os
import shutil

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
)

# Continue with the rest of the code...
