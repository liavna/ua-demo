from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

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
    'spark_job_dag',
    default_args=default_args,
    description='DAG to run PySpark job on Kubernetes',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
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

def run_spark_job():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, StructType, StructField
    import urllib.request
    import json

    url = "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    response = urllib.request.urlopen(url)
    data_list = json.loads(response.read())

    spark = SparkSession.builder.appName("AlertsToCSV").getOrCreate()

    if data_list:
        schema = StructType([
            StructField("alertDate", StringType(), True),
            StructField("data", StringType(), True)
        ])

        df = spark.createDataFrame(data_list, schema=schema)

        if df.columns:
            df_selected = df.select("alertDate", "data")

            if df_selected.count() > 0:
                output_path = "file:///mounts/shared-volume/alerts/data/"
                df_selected.write.mode("overwrite").option("header", "true").option("delimiter", ",").csv(output_path)
            else:
                print("DataFrame is empty after selecting columns.")
        else:
            print("DataFrame has no columns.")
    else:
        print("Data list is empty.")

    spark.stop()

run_spark_job_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag,
)

# You may need to adjust the KubernetesPodOperator parameters based on your Kubernetes setup
spark_job_task = KubernetesPodOperator(
    task_id="spark_job_task",
    image="your_spark_image:tag",
    cmds=["/bin/bash", "-c"],
    arguments=["/path/to/your/spark_script.py"],
    namespace="your_airflow_namespace",
    is_delete_pod_operator=True,
    dag=dag,
)

run_spark_job_task >> spark_job_task

