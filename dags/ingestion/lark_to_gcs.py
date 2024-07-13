import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utils.common.data_helper import DEFAULT_DATETIME_FORMAT, DEFAULT_DATE_PARTITON_FORMAT
from utils.lark import Lark

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")
SOURCE_NAME = 'lark'
CURRENT_DATE = datetime.utcnow().strftime(DEFAULT_DATE_PARTITON_FORMAT)
app_tokens = Variable.get('lark_app_tokens', default_var='[]', deserialize_json=True)


def _extract_and_dump_table_records(table_id, table_name, lark_app_token):
    lark_base = Lark(
        app_id=Variable.get('lark_app_id', default_var=None),
        app_secret=Variable.get('lark_app_secret', default_var=None),
        api_page_size=Variable.get('lark_api_page_size', default_var=20)
    )

    # ingest table from lark base
    records = lark_base.get_records(
        app_token=lark_app_token,
        table_id=table_id,
        table_name=table_name
    )
    df = pd.DataFrame([record.get('fields', []) for record in records])

    # Incremental mode
    if 'Last Modified Date' in df.columns:
        # Get ingestion information dictionary
        lark_ingestion_info = Variable.get('lark_ingestion_info', default_var={}, deserialize_json=True)
        table_ingestion_info = lark_ingestion_info.get(table_id, {})

        # max 'Last Modified Date' from prev ingestion
        prev_latest_datetime = table_ingestion_info.get('prev_latest_datetime', 0)
        # max 'Last Modified Date' from current ingestion
        latest_datetime = table_ingestion_info.get('latest_datetime', 0)
        # datetime from recent ingestion
        latest_ingestion_datetime = table_ingestion_info.get('latest_ingestion_datetime',
                                                             datetime.utcnow().strftime(DEFAULT_DATETIME_FORMAT))
        latest_ingestion_datetime = datetime.strptime(latest_ingestion_datetime, DEFAULT_DATETIME_FORMAT)

        latest_ingestion_date = latest_ingestion_datetime.date()
        current_date = datetime.utcnow().date()
        if latest_ingestion_date == current_date:
            offset_datetime = prev_latest_datetime
        else:
            offset_datetime = latest_datetime

        df = df[df['Last Modified Date'] > offset_datetime]

        if len(df) != 0:
            table_ingestion_info['latest_datetime'] = max(df['Last Modified Date'])
            table_ingestion_info['latest_ingestion_datetime'] = datetime.utcnow().strftime(DEFAULT_DATETIME_FORMAT)
            if latest_ingestion_date != current_date:
                table_ingestion_info['prev_latest_datetime'] = latest_datetime
            lark_ingestion_info[table_id] = table_ingestion_info
            Variable.set("lark_ingestion_info", lark_ingestion_info, serialize_json=True)

            # Save data to local
            output_dir = Path(f'/tmp/{SOURCE_NAME}')
            output_dir.mkdir(parents=True, exist_ok=True)
            df.to_csv(f'{output_dir}/{table_id}.csv')


def _local_to_gcs(table_id, bucket_name):
    hook = GCSHook()

    source_path = f'/tmp/{SOURCE_NAME}/{table_id}.csv'
    if os.path.isfile(source_path):
        hook.upload(
            bucket_name=bucket_name,
            object_name=f'lark/{table_id}/{CURRENT_DATE}/data.csv',
            mime_type='application/octet-stream',
            filename=source_path,
        )
        os.remove(source_path)


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

    for app_token in app_tokens:
        @task
        def get_tables_conf(lark_app_token):
            lark_base = Lark(
                app_id=Variable.get('lark_app_id', default_var=None),
                app_secret=Variable.get('lark_app_secret', default_var=None),
                api_page_size=Variable.get('lark_api_page_size', default_var=20)
            )

            return lark_base.get_tables(
                app_token=lark_app_token
            )


        @task_group
        def extract_and_load_tables(table_id, name, lark_app_token):
            extract_and_dump_records = PythonOperator(
                task_id=f'extract_and_dump_to_local',
                python_callable=_extract_and_dump_table_records,
                op_kwargs={
                    'table_id': table_id,
                    'table_name': name,
                    'lark_app_token': lark_app_token
                }
            )

            bucket_name = Variable.get('gcs_bucket_raw_name', default_var=None)
            local_to_gcs = PythonOperator(
                task_id=f'local_to_gcs',
                python_callable=_local_to_gcs,
                op_kwargs={
                    'table_id': table_id,
                    'bucket_name': bucket_name,
                }
            )

            extract_and_dump_records >> local_to_gcs


        tables_conf = get_tables_conf(app_token)
        el_tables = extract_and_load_tables.expand_kwargs(tables_conf)

        start_ingestion >> tables_conf >> el_tables >> end_ingestion
