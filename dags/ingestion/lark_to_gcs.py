import logging
from datetime import datetime, timedelta

import pandas as pd
import pytz
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utils.lark import Lark

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


def _extract_table_records(ti, table_id, table_name):
    lark_base = Lark(
        app_id=Variable.get('lark_app_id', default_var=None),
        app_secret=Variable.get('lark_app_secret', default_var=None),
        api_page_size=Variable.get('lark_api_page_size', default_var=20)
    )

    records = lark_base.get_records(
        app_token=Variable.get('lark_app_token', default_var=None),
        table_id=table_id,
        table_name=table_name
    )
    ti.xcom_push(key=f'{table_id}', value=records)


def _dump_table_data(ti, table_id):
    records = ti.xcom_pull(key=f'{table_id}', task_ids=f'extract_and_load_tables.extract')[0]

    df = pd.DataFrame([record.get('fields', []) for record in records])
    df.to_csv(f'/tmp/{table_id}.csv')


def _local_to_gcs(table_id, bucket_name):
    hook = GCSHook()

    hook.upload(
        bucket_name=bucket_name,
        object_name=f'{datetime.utcnow().replace(tzinfo=pytz.UTC).date()}/{table_id}.csv',
        mime_type='application/octet-stream',
        filename=f'/tmp/{table_id}.csv',
    )


default_args = {
    'start_date': datetime(2024, 6, 29),
    "email": ["lam.nguyen3@hebela.net"],
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@daily",
    "catchup": False
}
with DAG(
        dag_id='ingestion_lark_to_gcs',
        default_args=default_args,
) as dag:
    start_ingestion = EmptyOperator(task_id="start_ingestion")
    end_ingestion = EmptyOperator(task_id="end_ingestion")


    @task
    def get_tables_conf():
        lark_base = Lark(
            app_id=Variable.get('lark_app_id', default_var=None),
            app_secret=Variable.get('lark_app_secret', default_var=None),
            api_page_size=Variable.get('lark_api_page_size', default_var=20)
        )

        return lark_base.get_tables(
            app_token=Variable.get('lark_app_token', default_var=None)
        )


    @task_group
    def extract_and_load_tables(table_id, name):
        extract_records = PythonOperator(
            task_id=f'extract',
            python_callable=_extract_table_records,
            op_kwargs={
                'table_id': table_id,
                'table_name': name
            }
        )

        dump_records = PythonOperator(
            task_id=f'dump_to_local',
            python_callable=_dump_table_data,
            op_kwargs={
                'table_id': table_id,
                'table_name': name
            }
        )

        bucket_name = Variable.get('gcs_bucket_raw_name', default_var=None)
        load_records_to_gcs = PythonOperator(
            task_id=f'local_to_gcs',
            python_callable=_local_to_gcs,
            op_kwargs={
                'table_id': table_id,
                'bucket_name': bucket_name,
            }
        )

        extract_records >> dump_records >> load_records_to_gcs


    tables_conf = get_tables_conf()
    el_tables = extract_and_load_tables.expand_kwargs(tables_conf)

    start_ingestion >> tables_conf >> el_tables >> end_ingestion
