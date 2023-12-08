from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime 
from ptv_transformation import transform_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 6),
    'email': ['myemail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ptv_transform_dag',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule_interval="10 */6 * * *",
    description='Read files from S3, transform, and upload to BigQuery',
)

run_transform = PythonOperator(
    task_id='ptv_transform_disruptions',
    python_callable=transform_data,
    dag=dag
)

run_transform
