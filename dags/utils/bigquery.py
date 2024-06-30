import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import io
import os
from google.cloud import bigquery
import pyarrow
from google.api_core.exceptions import NotFound, TooManyRequests


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"""C:/Users/anhduc/Downloads/7. Data Visualation-20230915T063335Z-001/service_key_ggcloud.json"""


def get_data_from_gcs(bucket_name, area, model_name, model_type, file_names):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        data_frames = []

        for file_name in file_names:
            source_blob_name = f"{area}/{model_name}/{model_type}/{file_name}"
            blob = bucket.blob(source_blob_name)

            data = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(data))

            if df.empty:
                print(f"Warning: DataFrame is empty for file {file_name}.")
                continue

            df = df.dropna(how='all')
            data_frames.append(df)

        if data_frames:
            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df
        else:
            raise ValueError("No valid data frames to concatenate.")

    except Exception as e:
        print(f"Error occurred while fetching data from GCS: {e}")
        return None


def rename_columns(df, mapping):
    new_columns = []
    # Lấy những cột map được
    for old_column, new_column in mapping.items():
        if old_column in df.columns:
            new_columns.append(new_column)
            df.rename(columns={old_column: new_column}, inplace=True)
        else:
            print(f"Column '{old_column}' not found, skip renam.")

    return df[new_columns]


def upsert_to_bigquery(df, model, keys):
    if df.empty:
        raise ValueError("Dataframe is empty, can't load to BigQuery")

    client = bigquery.Client()

    # Tạo bảng staging
    stg_dataset = 'staging'
    stg_table_ref = client.dataset(stg_dataset).table(model)

    # DELETE IF EXIST
    try:
        stg_table = client.get_table(stg_table_ref)
        if stg_table is not None:
            client.delete_table(stg_table_ref)
            print(f"Deleted existing table {stg_table_ref.path}.")
    except NotFound:
        pass

    # CREATE TABLE
    try:
        dwh_dataset = 'dwh'
        dwh_table_ref = client.dataset(dwh_dataset).table(model)
        dwh_table = client.get_table(dwh_table_ref)
        dwh_schema = dwh_table.schema
        print(dwh_schema)

        stg_table = bigquery.Table(stg_table_ref, schema=dwh_schema)
        print(stg_table.schema)
        client.create_table(stg_table)
        print(f"Created table {model} in stage.")

        # Ghi đè dữ liệu vào bảng staging
        overwrite_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job_temp = client.load_table_from_dataframe(df, stg_table_ref, job_config=overwrite_config)
        job_temp.result()
        print(f"Successful overwrite {model} in staging.")

        gen_query_id = f"""
                        UPDATE `{stg_dataset}.{model}`
                        SET id = FARM_FINGERPRINT(CONCAT(CAST({keys[0]} AS STRING), CAST({keys[1]} AS STRING)))
                        WHERE 1 = 1;
                    """
        gen_query_job = client.query(gen_query_id)
        gen_query_job.result()

    except Exception as e:
        print(f'Error Create Table: {e}')

    try:
        # Thực hiện upsert vào bảng dwh
        merge_query = f"""
            MERGE `{dwh_dataset}.{model}` AS dest
            USING `{stg_dataset}.{model}` AS src
            ON dest.id = src.id
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f'dest.{col} = src.{col}' for col in df.columns])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(df.columns)}) VALUES ({', '.join([f'src.{col}' for col in df.columns])})
        """

        query_job = client.query(merge_query)
        query_job.result()

        print(f"Successful upsert {dwh_dataset}.{model}.")

    except TooManyRequests as e:
        print(f"Too many requests. Error: {e}")
        # Chưa cần thiết Retry

    except Exception as e:
        print(f"Error Upsert: {e}")


def get_files_recent_partition(model_name, num_periods, type_of_time="months", file_extension=".csv"):
    now = datetime.now()
    file_names = []

    for i in range(num_periods):
        if type_of_time == "months":
            period = (now - relativedelta(months=i)).strftime("%Y%m")

        elif type_of_time == "days":
            period = (now - timedelta(days=i)).strftime("%Y%m%d")

        else:
            raise ValueError("must be either 'months' or 'days'")

        file_name = f"{model_name}_{period}{file_extension}"
        file_names.append(file_name)

    return file_names