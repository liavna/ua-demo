
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
    ccess_control={
		'All': {
			'can_read',
			'can_edit',
			'can_delete'
		}
	}
}
def creatableLoad():

    try:
        dbconnect = pg.connect(
            "dbname='dezyre_new' user='postgres' host='localhost' password='root'"
        )
    except Exception as error:
        print(error)
    
    # create the table if it does not already exist
    cursor = dbconnect.cursor()
    cursor.execute("""
         CREATE TABLE IF NOT EXISTS drivers_data (
            school_year varchar(50),
            vendor_name varchar(50),
            type_of_service varchar(50),
            active_employees varchar(50),
            job_type varchar(50)
        );
        
        TRUNCATE TABLE drivers_data;
    """
    )
    dbconnect.commit()
    
    # insert each csv row as a record in our database
    with open('/home/hduser/drivers.csv', 'r') as f:
        next(f)  # skip the first row (header)     
        for row in f:
            cursor.execute("""
                INSERT INTO drivers_data
                VALUES ('{}', '{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3],
            row.split(",")[4])
            )
    dbconnect.commit()
dag_pandas = DAG(
	dag_id = "using_pandas_demo",
	default_args=default_args ,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = airflow.utils.dates.days_ago(1))