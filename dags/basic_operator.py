from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return 'Hello world!'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 3),
    'retries': 1
}
with DAG(
    'simple_dag',
    default_args=default_args,
    schedule_interval=None
) as dag:
    start_operator = DummyOperator(task_id='start_task', dag=dag)
    hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
    end_operator = DummyOperator(task_id='end_task', dag=dag)

    start_operator >> hello_operator >> end_operator
