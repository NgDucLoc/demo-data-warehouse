import ast
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from hashlib import md5
from typing import Dict, List, Optional

import pandas as pd
from pandas import DataFrame

from utils.common.data_helper import read_gcs_table, save_table_to_gbq, preprocess_bronze_data
from utils.common.schema_helper import apply_schema_to_df
from utils.dwh.bronze.lark.attendance import TBL_ATTENDANCE, RENAME_ATTENDANCE_COLS
from utils.dwh.bronze.lark.attendance_record import TBL_ATTENDANCE_RECORD, RENAME_ATTENDANCE_RECORD_COLS
from utils.dwh.bronze.lark.employee import TBL_EMPLOYEE, RENAME_EMPLOYEE_COLS

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


@dataclass
class DataSet:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    database: str
    partition: str
    skip_publish: bool = False
    replace_partition: bool = False


class StandardETL(ABC):
    def __init__(
            self,
            raw_bucket: Optional[str] = None,
            raw_storage_path: Optional[str] = None,
            database: Optional[str] = None,
            partition: Optional[str] = None,
    ):
        self.RAW_BUCKET = raw_bucket
        self.RAW_STORAGE_PATH = raw_storage_path
        self.DATABASE = database
        self.DEFAULT_PARTITION = partition or datetime.now().strftime(
            "%Y-%m-%d-%H-%M-%S"
        )
        self.DEFAULT_FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S'

    def check_required_inputs(
            self, input_datasets: Dict[str, DataSet], required_ds: List[str]
    ) -> None:
        if not all([ds in input_datasets for ds in required_ds]):
            raise ValueError(
                f"The input_datasets {input_datasets.keys()} does not contain"
                f" {required_ds}"
            )

    def publish_data(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> None:
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                curr_data = input_dataset.curr_data
                curr_data['etl_inserted'] = pd.Timestamp.utcnow()
                curr_data['partition_value'] = input_dataset.partition

                save_table_to_gbq(
                    client_gbq=kwargs.get('client_gbq'),
                    database_name=input_dataset.database,
                    table_name=input_dataset.table_name,
                    data_df=curr_data,
                    replace_partition=input_dataset.replace_partition,
                    primary_keys=input_dataset.primary_keys
                )

    @abstractmethod
    def get_bronze_datasets(
            self, **kwargs
    ) -> Dict[str, DataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> Dict[str, DataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> Dict[str, DataSet]:
        pass

    def run(self, **kwargs):
        bronze_data_sets = self.get_bronze_datasets(**kwargs)
        self.publish_data(bronze_data_sets, **kwargs)
        task_logger.info(
            'Created & published bronze datasets:'
            f' {[ds for ds in bronze_data_sets.keys()]}'
        )

        silver_data_sets = self.get_silver_datasets(
            bronze_data_sets, **kwargs
        )
        self.publish_data(silver_data_sets, **kwargs)
        logging.info(
            'Created & published silver datasget_silver_datasetsets:'
            f' {[ds for ds in silver_data_sets.keys()]}'
        )

        gold_data_sets = self.get_gold_datasets(
            silver_data_sets, **kwargs
        )
        self.publish_data(gold_data_sets, **kwargs)
        logging.info(
            'Created & published gold datasets:'
            f' {[ds for ds in gold_data_sets.keys()]}'
        )


class LarkETL(StandardETL):
    def get_bronze_datasets(
            self, **kwargs
    ) -> Dict[str, DataSet]:
        partition = kwargs.get('partition', self.DEFAULT_PARTITION)

        # Load table Employee from GCS
        employee_df = read_gcs_table(
            client_gcs=kwargs.get('client_gcs'),
            bucket_name=self.RAW_BUCKET,
            table_path=f"lark/tbllYZkNjgSHIcmT/{partition}/data.csv"
        )
        # Common processing for table
        employee_df = preprocess_bronze_data(
            data_df=employee_df,
            tbl_cols_dict=TBL_EMPLOYEE,
            rename_cols_dict=RENAME_EMPLOYEE_COLS
        )
        employee_df.dropna(subset=['user_id'], inplace=True)

        # Load table Attendance Record from GCS
        attendance_record_df = read_gcs_table(
            client_gcs=kwargs.get('client_gcs'),
            bucket_name=self.RAW_BUCKET,
            table_path=f"lark/tblPQIgHsv2W2Wq3/{partition}/data.csv"
        )
        # Common processing for table
        attendance_record_df = preprocess_bronze_data(
            data_df=attendance_record_df,
            tbl_cols_dict=TBL_ATTENDANCE_RECORD,
            rename_cols_dict=RENAME_ATTENDANCE_RECORD_COLS
        )

        # Load table Attendance from GCS
        attendance_df = read_gcs_table(
            client_gcs=kwargs.get('client_gcs'),
            bucket_name=self.RAW_BUCKET,
            table_path=f"lark/tblZcqZFnoyzu913/{partition}/data.csv"
        )
        # Common processing for table
        attendance_df = preprocess_bronze_data(
            data_df=attendance_df,
            tbl_cols_dict=TBL_ATTENDANCE,
            rename_cols_dict=RENAME_ATTENDANCE_COLS
        )

        return {
            'employee': DataSet(
                name='employee',
                curr_data=employee_df,
                primary_keys=['user_id', 'partition_value'],
                storage_path='',
                table_name='lark_employee',
                data_type='bigquery',
                database='bronze',
                partition=partition,
                replace_partition=True,
            ),
            'attendance_record': DataSet(
                name='attendance_record',
                curr_data=attendance_record_df,
                primary_keys=['record_id', 'partition_value'],
                storage_path='',
                table_name='lark_attendance_record',
                data_type='bigquery',
                database='bronze',
                partition=partition,
                replace_partition=True,
            ),
            'attendance': DataSet(
                name='attendance',
                curr_data=attendance_df,
                primary_keys=['attendance_id', 'partition_value'],
                storage_path='',
                table_name='lark_attendance',
                data_type='bigquery',
                database='bronze',
                partition=partition,
                replace_partition=True,
            )
        }

    def get_dim_employee(
            self, employee: DataSet, **kwargs
    ) -> DataFrame:
        employee_df = employee.curr_data
        employee_df['employee_sur_id'] = employee_df.apply(
            lambda item: md5((item['user_id'] + item['datetime_updated'].strftime(self.DEFAULT_FORMAT_DATETIME)).encode(
                'utf-8')).hexdigest(), axis=1)
        employee_df['user'] = employee_df['user'].apply(
            lambda item: ast.literal_eval(item) if isinstance(item, str) else None)
        employee_df['lark_id'] = employee_df['user'].apply(
            lambda item: item[0].get('id', None) if isinstance(item, list) else None)
        employee_df['leader'] = employee_df['leader'].apply(
            lambda item: ast.literal_eval(item) if isinstance(item, str) else None)
        employee_df['leader_lark_id'] = employee_df['leader'].apply(
            lambda item: item[0].get('id', None) if isinstance(item, list) else None)
        employee_df['name'] = employee_df['user'].apply(
            lambda item: item[0].get('name', None) if isinstance(item, list) else None)
        employee_df['department_ids'] = employee_df['department_ids'].apply(
            lambda item: ast.literal_eval(item) if isinstance(item, str) else None)

        # get only latest customer rows in dim_customer
        # since dim customer may have multiple rows per customer (SCD2)
        dim_employee_latest = kwargs['dim_employee']

        lark_to_user_dict_latest = dict(zip(dim_employee_latest['lark_id'], dim_employee_latest['employee_sur_id']))
        lark_to_user_dict_new = dict(zip(employee_df['lark_id'], employee_df['employee_sur_id']))
        employee_df['leader_sur_id'] = employee_df['leader_lark_id'].apply(
            lambda item: lark_to_user_dict_new.get(item, None) or lark_to_user_dict_latest.get(item, None))
        employee_df = employee_df[
            [col for col in dim_employee_latest.keys() if col not in ['valid_from', 'valid_to', 'is_current']]]

        # get net new rows to insert
        employee_df_insert_net_new = pd.merge(employee_df, dim_employee_latest, how='left', on=['user_id'],
                                              suffixes=('', '_latest'))
        employee_df_insert_net_new = employee_df_insert_net_new[
            pd.isnull(employee_df_insert_net_new['datetime_updated_latest'])]
        employee_df_insert_net_new = employee_df_insert_net_new[employee_df.columns]
        employee_df_insert_net_new['is_current'] = True
        employee_df_insert_net_new['valid_from'] = employee_df_insert_net_new['datetime_updated']
        employee_df_insert_net_new['valid_to'] = datetime.strptime('2099-01-01 12:00:00', self.DEFAULT_FORMAT_DATETIME)

        # get rows to insert for existing ids
        employee_df_insert_existing_ids = pd.merge(employee_df, dim_employee_latest, how='inner', on=['user_id'],
                                                   suffixes=('', '_latest'))
        employee_df_insert_existing_ids = employee_df_insert_existing_ids[(
                employee_df_insert_existing_ids['datetime_updated_latest'] < employee_df_insert_existing_ids[
            'datetime_updated'])]
        employee_df_insert_existing_ids = employee_df_insert_existing_ids[employee_df.columns]
        employee_df_insert_existing_ids['is_current'] = True
        employee_df_insert_existing_ids['valid_from'] = employee_df_insert_existing_ids['datetime_updated']
        employee_df_insert_existing_ids['valid_to'] = datetime.strptime('2099-01-01 12:00:00',
                                                                        self.DEFAULT_FORMAT_DATETIME)

        # get rows to be updated
        employee_df_ids_update = pd.merge(employee_df, dim_employee_latest, how='inner', on=['user_id'],
                                          suffixes=('_new', ''))
        employee_df_ids_update = employee_df_ids_update[(
                employee_df_ids_update['datetime_updated'] < employee_df_ids_update[
            'datetime_updated_new'])]
        employee_df_ids_update['datetime_updated'] = employee_df_ids_update['datetime_updated_new']
        employee_df_ids_update = employee_df_ids_update[employee_df.columns]
        employee_df_ids_update['is_current'] = False
        employee_df_ids_update['valid_to'] = employee_df_ids_update['datetime_updated']

        dim_employee_df = pd.concat(
            [employee_df_insert_net_new, employee_df_insert_existing_ids, employee_df_ids_update],
            ignore_index=True)

        # Apply schema from GBQ
        dim_employee_df = apply_schema_to_df(
            client_gbq=kwargs.get('client_gbq'),
            dataset_name='silver',
            table_name='dim_employee',
            data_df=dim_employee_df
        )

        return dim_employee_df

    def get_fact_attendance_record(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> DataFrame:
        self.check_required_inputs(input_datasets, ['dim_employee', 'attendance_record'])

        dim_employee = input_datasets['dim_employee'].curr_data
        attendance_record_df = input_datasets['attendance_record'].curr_data
        fact_attendance_record_df = pd.merge(attendance_record_df, dim_employee, how='left', on=['user_id'],
                                             suffixes=['', '_right'])

        # Apply schema from GBQ
        fact_attendance_record_df = apply_schema_to_df(
            client_gbq=kwargs.get('client_gbq'),
            dataset_name='silver',
            table_name='fact_attendance_record',
            data_df=fact_attendance_record_df
        )

        return fact_attendance_record_df

    def get_fact_attendance(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> DataFrame:
        self.check_required_inputs(input_datasets, ['dim_employee', 'attendance'])

        dim_employee = input_datasets['dim_employee'].curr_data
        attendance_df = input_datasets['attendance'].curr_data
        fact_attendance_df = pd.merge(attendance_df, dim_employee, how='left', on=['user_id'],
                                      suffixes=['', '_right'])

        # Apply schema from GBQ
        fact_attendance_df = apply_schema_to_df(
            client_gbq=kwargs.get('client_gbq'),
            dataset_name='silver',
            table_name='fact_attendance',
            data_df=fact_attendance_df
        )

        return fact_attendance_df

    def get_silver_datasets(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> Dict[str, DataSet]:
        self.check_required_inputs(input_datasets, ['employee', 'attendance_record'])
        dim_employee_df = self.get_dim_employee(
            input_datasets['employee'],
            dim_employee=pd.read_gbq("SELECT * FROM silver.dim_employee WHERE is_current = True", dialect="standard",
                                     credentials=kwargs.get('credentials', None)),
            client_gbq=kwargs.get('client_gbq')
        )

        silver_datasets = {}
        silver_datasets['dim_employee'] = DataSet(
            name='dim_employee',
            curr_data=dim_employee_df,
            primary_keys=['employee_sur_id'],
            storage_path='',
            table_name='dim_employee',
            data_type='',
            database='silver',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
        )
        self.publish_data(silver_datasets, **kwargs)
        silver_datasets['dim_employee'].curr_data = pd.read_gbq(
            "SELECT * FROM silver.dim_employee WHERE is_current = True", dialect="standard",
            credentials=kwargs.get('credentials', None))
        silver_datasets['dim_employee'].skip_publish = True
        input_datasets['dim_employee'] = silver_datasets['dim_employee']

        silver_datasets['fact_attendance_record'] = DataSet(
            name='fact_attendance_record',
            curr_data=self.get_fact_attendance_record(input_datasets, **kwargs),
            primary_keys=['record_id'],
            storage_path='',
            table_name='fact_attendance_record',
            data_type='',
            database='silver',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        silver_datasets['fact_attendance'] = DataSet(
            name='fact_attendance',
            curr_data=self.get_fact_attendance(input_datasets, **kwargs),
            primary_keys=['attendance_id'],
            storage_path='',
            table_name='fact_attendance',
            data_type='',
            database='silver',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
            replace_partition=True,
        )

        return silver_datasets

    def get_gold_datasets(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> Dict[str, DataSet]:
        self.check_required_inputs(input_datasets, ['dim_employee', 'fact_attendance'])

        dim_employee_df = input_datasets['dim_employee'].curr_data
        fact_attendance_df = input_datasets['fact_attendance'].curr_data
        merged_df = fact_attendance_df.merge(dim_employee_df, on=['user_id'], how='left')

        cube_attendance_report_df = pd.DataFrame()
        cube_attendance_report_df['attendance_month'] = merged_df['attendance_date'].dt.strftime('%Y-%m')
        cube_attendance_report_df['attendance_date'] = merged_df['attendance_date'].dt.strftime('%Y-%m-%d')
        cube_attendance_report_df['lark_hrm_code'] = merged_df['user_id']
        cube_attendance_report_df['hrm_name'] = merged_df['name']
        cube_attendance_report_df['job_title'] = merged_df['job_title']
        datetime_col = ['check_in_datetime', 'check_out_datetime', 'check_out_shift_time', 'check_in_shift_time']
        merged_df[datetime_col] = merged_df[datetime_col].apply(
            lambda x: pd.to_datetime(x, format='%d/%m/%Y %H:%M', errors='coerce'))
        cube_attendance_report_df['late_time_minute'] = (
                (merged_df['check_in_datetime'] + pd.Timedelta(hours=7) - merged_df[
                    'check_in_shift_time']).dt.total_seconds() / 60
        ).clip(upper=0).abs().fillna(0)
        cube_attendance_report_df['early_time_minute'] = (
                (merged_df['check_out_datetime'] + pd.Timedelta(hours=7) - merged_df[
                    'check_out_shift_time']).dt.total_seconds() / 60
        ).clip(upper=0).abs().fillna(0)
        cube_attendance_report_df['working_duration_hours'] = (
                (merged_df['check_out_datetime'] - merged_df['check_in_datetime']).dt.total_seconds() / 3600
        ).fillna(0)
        cube_attendance_report_df['working_duration_benchmark'] = (
                (merged_df['check_out_shift_time'] - merged_df['check_in_shift_time']).dt.total_seconds() / 3600
        ).fillna(0)
        cube_attendance_report_df['penalty_amount'] = merged_df['penalty']
        cube_attendance_report_df.dropna(how='all', inplace=True)

        # Apply schema from GBQ
        cube_attendance_report_df = apply_schema_to_df(
            client_gbq=kwargs.get('client_gbq'),
            dataset_name='gold',
            table_name='cube_attendance_report',
            data_df=cube_attendance_report_df
        )

        cube_attendance_report = DataSet(
            name='cube_attendance_report',
            curr_data=cube_attendance_report_df,
            primary_keys=['lark_hrm_code', 'attendance_date'],
            storage_path='',
            table_name='cube_attendance_report',
            data_type='',
            database='gold',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
            replace_partition=True,
        )

        return {'cube_attendance_report': cube_attendance_report}
