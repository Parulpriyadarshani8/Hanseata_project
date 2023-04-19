from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'parul',
    'start_date': datetime(2023, 4, 11),
    'retries': 0
    
}

def greet():
    print("hello world")



with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v01',
    description='Our first dag using python operator',
    schedule_interval='@daily'
) as dag:


    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task1