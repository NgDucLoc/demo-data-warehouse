import logging
import math
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from .schema_helper import get_gbq_table_schema

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

# Init constants
DEFAULT_DATE_FORMAT = '%Y%m%d'
DEFAULT_DATE_PARTITON_FORMAT = '%Y-%m-%d'
FULL_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
GMT_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
SECONDS_IN_DAY = 60 * 60 * 24
SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_MINUTE = 60


def read_gcs_table(client_gcs, bucket_name, table_path):
    bucket = client_gcs.get_bucket(bucket_name)
    blob = bucket.blob(table_path)

    if blob.exists():
        table_df = pd.read_csv(blob.open('r', encoding="utf8"), index_col=0)
    else:
        table_df = None

    return table_df


def check_table_exists(client_gbq, table_ref):
    try:
        client_gbq.get_table(table_ref)
        return True
    except NotFound:
        return False


def save_table_to_gbq(client_gbq, database_name, table_name, data_df, replace_partition=False,
                      primary_keys=[]):
    data_df['partition_value'] = pd.to_datetime(data_df['partition_value'], utc=True).dt.date
    data_df['partition_value'] = data_df['partition_value'].astype('datetime64[ns, UTC]')

    target_table_schema = get_gbq_table_schema(
        client_gbq=client_gbq,
        dataset_name=database_name,
        table_name=table_name
    )
    target_schema = [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field in
                     target_table_schema]

    if replace_partition:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='partition_value'
            ),
            schema=target_schema
        )
        table_ref = client_gbq.dataset(database_name).table(table_name)
        job = client_gbq.load_table_from_dataframe(
            data_df, table_ref, job_config=job_config
        )
        job.result()
    else:
        # Pre-check to create temp table
        temp_table_id = f'temp.{table_name}'
        temp_table_ref = client_gbq.dataset('temp').table(table_name)
        temp_table = bigquery.Table(temp_table_ref, schema=target_table_schema)
        if not check_table_exists(client_gbq, temp_table_ref):
            client_gbq.create_table(temp_table, exists_ok=True)

        # Write data to temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=target_schema
        )
        client_gbq.load_table_from_dataframe(data_df, temp_table_id, job_config=job_config).result()

        # Construct SQL MERGE INTO statement
        merge_query = f"""
            MERGE INTO `{database_name}.{table_name}` target
            USING `{temp_table_id}` source
            ON {' AND '.join(f'target.{key} = source.{key}' for key in primary_keys)}
            WHEN MATCHED THEN
                UPDATE SET {', '.join(f"target.{col} = source.{col}" for col in data_df.columns)}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(data_df.columns)})
                VALUES ({', '.join(f"source.{col}" for col in data_df.columns)})
            """
        job = client_gbq.query(merge_query)
        job.result()

        # Freeze temp table
        if check_table_exists(client_gbq, temp_table_ref):
            client_gbq.delete_table(temp_table)


def preprocess_bronze_data(data_df, tbl_cols_dict, rename_cols_dict):
    for col_name, col_type in tbl_cols_dict.items():
        # Set None for col (lark: no data no column).
        if col_name not in data_df.columns:
            data_df[col_name] = None

        # Convert Timestamp to Datetime format
        if col_type == 'datetime64[ns, UTC]':
            data_df[col_name] = data_df[col_name].apply(
                lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (
                    float, int) and not math.isnan(
                    item) else None)
            data_df[col_name] = pd.to_datetime(data_df[col_name], utc=True)

        # Convert Timestamp to Datetime format
        if col_type in ['int64']:
            data_df[col_name] = data_df[col_name].apply(
                lambda item: item[0] if isinstance(item, list) else 0)

    data_df = data_df[tbl_cols_dict.keys()].astype(tbl_cols_dict)
    data_df = data_df.rename(columns=rename_cols_dict)

    return data_df


def get_bigquery_connection(service_account_json):
    credentials = service_account.Credentials.from_service_account_info(service_account_json)
    client_gcs = storage.Client(credentials=credentials)
    client_gbq = bigquery.Client(credentials=credentials)

    return client_gcs, client_gbq, credentials
