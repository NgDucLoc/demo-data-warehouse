import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, Connection
from airflow.operators.empty import EmptyOperator
from utils.common.data_helper import get_bigquery_connection
from utils.etl import LarkETL
from utils.notifier import LarkChatNotifier

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")
ETL_RUN_DATE = Variable.get('etl_run_date', default_var=None)
SOURCE_NAME = 'lark'
PARTITION_FORMAT = '%Y-%m-%d'

default_args = {
    'start_date': datetime(2024, 7, 16),
    "email": ["lam.nguyen3@hebela.net"],
    "retry_delay": timedelta(minutes=5)
}
with DAG(
        dag_id='run_etl',
        default_args=default_args,
        catchup=False,
        schedule_interval="*/5 * * * *",
        on_success_callback=LarkChatNotifier(message="Thành công chạy etl trên dữ liệu Lark"),
        on_failure_callback=LarkChatNotifier(message="Thất bại chạy etl trên dữ liệu Lark"),
) as dag:
    start_etl = EmptyOperator(task_id="start_etl")
    end_etl = EmptyOperator(task_id="end_etl")


    @task
    def run_etl(**context):
        raw_bucket = Variable.get('gcs_bucket_raw_name', default_var=None)
        raw_storage_path = f'gs://{raw_bucket}/{SOURCE_NAME}'
        execution_date = context['data_interval_end']

        lark_etl = LarkETL(
            raw_bucket=raw_bucket,
            raw_storage_path=raw_storage_path
        )
        partition = execution_date.strftime(PARTITION_FORMAT) if not ETL_RUN_DATE else ETL_RUN_DATE
        gcloud_conn = Connection.get_connection_from_secrets(
            conn_id='google_cloud_default'
        )
        service_account_info = json.loads(json.loads(gcloud_conn.as_json()).get('extra', {}).get('keyfile_dict', ''))

        client_gcs, client_gbq, credentials = get_bigquery_connection(service_account_info)
        lark_etl.run(client_gcs=client_gcs, client_gbq=client_gbq, credentials=credentials, partition=partition)


    start_etl >> run_etl() >> end_etl
