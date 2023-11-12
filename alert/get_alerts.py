
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
}
def getDataToLocal():  
    
    url = "https://www.mako.co.il/Collab/amudanan/alerts.json"
    response = requests.get(url)

    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("school_year")

    df.to_csv("/home/hduser/drivers.csv", sep=',' ,escapechar='\\', quoting=csv.QUOTE_ALL, encoding='utf-8' )