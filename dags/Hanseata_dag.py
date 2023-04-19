from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import sys 
import os

sys.path.append('/opt/airflow_docker/dags/Hanseata/')
from Hanseata import Hanseata_data

# sys.path.append(os.path.dirname(os.path.abspath("Hanseata_data.py")))



default_args = {
    'owner': 'priyadarshani',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='Hanseata DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_hanseata_etl',
    python_callable=Hanseata_data.move_data_to_blob,
    dag=dag, 
)

run_etl