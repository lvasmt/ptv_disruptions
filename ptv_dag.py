from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime 
from etl_ptv_disruptions import run_ptv_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,9,29),
    'email': ['myemail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

dag = DAG(
    'ptv_disruptions_dag',
    default_args = default_args,
    max_active_runs=1,
    catchup=False, 
    schedule = "*/30 * * * *",
    description = 'Loading PTV disruptions to S3'
)

run_etl = PythonOperator(
    task_id='ptv_load_latest_disruptions',
    python_callable = run_ptv_dag,
    dag = dag
)

run_etl
