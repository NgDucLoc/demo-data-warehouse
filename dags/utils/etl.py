import ast
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from hashlib import md5
from typing import Dict, List, Optional

import pandas as pd
from pandas import DataFrame

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
                curr_data['partition_date'] = input_dataset.partition

                target_table_ref = kwargs.get('client_gbq').dataset(input_dataset.database).table(
                    input_dataset.table_name)
                target_table = kwargs.get('client_gbq').get_table(target_table_ref)
                target_table_schema = target_table.schema
                target_table_schema = [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field
                                       in target_table_schema]

                if input_dataset.replace_partition:
                    query = f"""
                    DELETE FROM `{input_dataset.database}.{input_dataset.table_name}`
                    WHERE partition_date = '{input_dataset.partition}'
                    """
                    kwargs.get('client_gbq').query(query).result()
                    curr_data.to_gbq(f'{input_dataset.database}.{input_dataset.table_name}', if_exists='append',
                                     credentials=kwargs.get('credentials'), table_schema=target_table_schema)
                else:
                    source_df = input_dataset.curr_data
                    target_df = pd.read_gbq(f"select * from {input_dataset.database}.{input_dataset.table_name}",
                                            credentials=kwargs.get('credentials'))
                    target_df = target_df.astype(source_df.dtypes)

                    merged_df = pd.merge(target_df, source_df, how='outer', on=input_dataset.primary_keys,
                                         suffixes=('_target', '_source'))
                    for col in source_df.columns:
                        if col in input_dataset.primary_keys:
                            continue
                        merged_df[col] = merged_df[col + '_source'].combine_first(merged_df[col + '_target'])
                    merged_df = merged_df[source_df.columns]
                    merged_df = merged_df.astype(source_df.dtypes)
                    merged_df['partition_date'] = pd.to_datetime(merged_df['partition_date'], utc=True)

                    merged_df.to_gbq(f'{input_dataset.database}.{input_dataset.table_name}', if_exists='replace',
                                     credentials=kwargs.get('credentials'), table_schema=target_table_schema)

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
        raw_bucket = kwargs.get('client_gcs').get_bucket(self.RAW_BUCKET)
        partition = kwargs.get('partition', self.DEFAULT_PARTITION)

        # Employee
        employee_col_types = {
            'user_id': 'string',
            'lark_id': 'string',
            'leader_lark_id': 'string',
            'name': 'string',
            'gender': 'string',
            'city': 'string',
            'email': 'string',
            'mobile': 'string',
            'job_title': 'string',
            'employee_type': 'string',
            'join_time': 'datetime64[ns, UTC]',
            'employee_no': 'string',
            'department_ids': 'object',
            'datetime_created': 'datetime64[ns, UTC]',
            'datetime_updated': 'datetime64[ns, UTC]',
            'etl_inserted': 'datetime64[ns, UTC]',
            'partition_date': 'datetime64[ns, UTC]'
        }
        col_mappings = {
            'Date Created': 'datetime_created',
            'Last Modified Date': 'datetime_updated'
        }
        employee_blob = raw_bucket.blob(f"lark/tbllYZkNjgSHIcmT/{partition}/data.csv")
        employee_df = pd.read_csv(employee_blob.open('r', encoding="utf8"), index_col=0)
        employee_df = employee_df.rename(columns=col_mappings)

        # Set null for col not have data.
        for col in employee_col_types.keys():
            if col not in employee_df.columns:
                employee_df[col] = None
        if 'leader' not in employee_df.columns:
            employee_df['leader'] = None

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
        employee_df['datetime_created'] = employee_df['datetime_created'].apply(
            lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (float, int) and not math.isnan(
                item) else None)
        employee_df['datetime_updated'] = employee_df['datetime_updated'].apply(
            lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (float, int) and not math.isnan(
                item) else None)
        employee_df['join_time'] = employee_df['join_time'].apply(
            lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (float, int) and not math.isnan(
                item) else None)

        employee_df.dropna(subset=['user_id'], inplace=True)
        employee_df = employee_df[employee_col_types.keys()]
        for col, col_type in employee_col_types.items():
            if col_type == 'datetime64[ns, UTC]':
                employee_df[col] = pd.to_datetime(employee_df[col], utc=True)
            else:
                employee_df[col] = employee_df[col].astype(col_type)

        # Attendance Record
        attendance_record_col_types = {
            'attendance_record_id': 'string',
            'user_id': 'string',
            'lark_id': 'string',
            'check_time': 'datetime64[ns, UTC]',
            'check_location': 'string',
            'is_offsite': 'bool',
            'attendance_date': 'datetime64[ns, UTC]',
            'etl_inserted': 'datetime64[ns, UTC]',
            'partition_date': 'datetime64[ns, UTC]'
        }
        col_mappings = {
            'Check location name': 'check_location',
            'Check time': 'check_time',
            'Date': 'attendance_date',
            'Is offsite': 'is_offsite',
            'Record id': 'attendance_record_id',
            'User id': 'user_id',
            'Employee': 'user'
        }
        attendance_record_blob = raw_bucket.blob(f"lark/tblPQIgHsv2W2Wq3/{partition}/data.csv")
        attendance_record_df = pd.read_csv(attendance_record_blob.open('r', encoding="utf8"), index_col=0)
        attendance_record_df = attendance_record_df.rename(columns=col_mappings)

        # Set null for col not have data.
        for col in attendance_record_col_types.keys():
            if col not in attendance_record_df.columns:
                attendance_record_df[col] = None

        attendance_record_df['user'] = attendance_record_df['user'].apply(
            lambda item: ast.literal_eval(item) if isinstance(item, str) else item)
        attendance_record_df['lark_id'] = attendance_record_df['user'].apply(
            lambda item: item[0].get('id', None) if isinstance(item, list) else None)
        attendance_record_df['check_time'] = attendance_record_df['check_time'].apply(
            lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (float, int) and not math.isnan(
                item) else None)
        attendance_record_df['attendance_date'] = attendance_record_df['attendance_date'].apply(
            lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (float, int) and not math.isnan(
                item) else None)

        attendance_record_df = attendance_record_df[attendance_record_col_types.keys()]
        for col, col_type in attendance_record_col_types.items():
            if col_type == 'datetime64[ns, UTC]':
                attendance_record_df[col] = pd.to_datetime(attendance_record_df[col], utc=True)
            else:
                attendance_record_df[col] = attendance_record_df[col].astype(col_type)

        # Attendance
        attendance_col_types = {
            'attendance_id': 'string',
            'attendance_date': 'datetime64[ns, UTC]',
            'user_id': 'string',
            'lark_id': 'string',
            'employee_type': 'string',
            'group_name': 'string',
            'shift_name': 'string',
            'check_in_record_id': 'string',
            'check_in_shift_time': 'datetime64[ns, UTC]',
            'datetime_check_in': 'datetime64[ns, UTC]',
            'check_in_location_name': 'string',
            'check_in_is_offsite': 'boolean',
            'check_in_type': 'string',
            'check_in_result': 'string',
            'check_in_result_supplement': 'string',
            'check_out_record_id': 'string',
            'check_out_shift_time': 'datetime64[ns, UTC]',
            'datetime_check_out': 'datetime64[ns, UTC]',
            'check_out_location_name': 'string',
            'check_out_is_offsite': 'boolean',
            'check_out_type': 'string',
            'check_out_result': 'string',
            'check_out_result_supplement': 'string',
            'request_penalty': 'boolean',
            'early_late': 'boolean',
            'early_late_20_min': 'boolean',
            'penalty_early_late_20_min': 'int64',
            'penalty': 'int64',
            'reason': 'string'
        }
        col_mappings = {
            'Result id': 'attendance_id',
            'User id': 'user_id',
            'Employee': 'employee',
            'Employee type': 'employee_type',
            'Date': 'attendance_date',
            'Group name': 'group_name',
            'Shift name': 'shift_name',
            'Check in record id': 'check_in_record_id',
            'Check in time': 'datetime_check_in',
            'Check in shift time': 'check_in_shift_time',
            'Check in location name': 'check_in_location_name',
            'Check in - Is offsite': 'check_in_is_offsite',
            'Check in type': 'check_in_type',
            'Check in result': 'check_in_result',
            'Check in result supplement': 'check_in_result_supplement',
            'Check out record id': 'check_out_record_id',
            'Check out time': 'datetime_check_out',
            'Check out shift time': 'check_out_shift_time',
            'Check out location name': 'check_out_location_name',
            'Check out - Is offsite': 'check_out_is_offsite',
            'Check out type': 'check_out_type',
            'Check out result': 'check_out_result',
            'Check out result supplement': 'check_out_result_supplement',
            'Nhân sự không đồng ý phiếu phạt': 'request_penalty',
            'Giá phạt đi muộn/ về sớm': 'early_late_price',
            'Muộn 20p/sớm 20p': 'early_late_20_min',
            'Phạt muộn 20p/sớm 20p': 'penalty_early_late_20_min',
            'Đi muộn / về sớm': 'early_late',
            'Tiền phạt': 'penalty',
            'Lý do': 'reason'
        }
        employee_type_mappings = {
            'optrBomdAH': 'Thử việc',
            'optv4rP0DT': 'Chính thức'
        }

        attendance_blob = raw_bucket.blob(f"lark/tblZcqZFnoyzu913/{partition}/data.csv")
        attendance_df = pd.read_csv(attendance_blob.open('r', encoding="utf8"), index_col=0)
        attendance_df = attendance_df.rename(columns=col_mappings)

        # Set null for col not have data.
        for col in attendance_col_types.keys():
            if col not in attendance_df.columns:
                attendance_df[col] = None

        attendance_df['employee'] = attendance_df['employee'].apply(
            lambda item: ast.literal_eval(item) if isinstance(item, str) else item)
        attendance_df['lark_id'] = attendance_df['employee'].apply(
            lambda item: item[0].get('id', None) if isinstance(item, list) else '')
        attendance_df['employee_type'] = attendance_df['employee_type'].apply(
            lambda item: employee_type_mappings.get(ast.literal_eval(item)[0]) if isinstance(item, str) and isinstance(
                ast.literal_eval(item),
                list) else '')
        attendance_df['early_late_price'] = attendance_df['early_late_price'].apply(
            lambda item: item[0] if isinstance(item, list) else 0)
        attendance_df['penalty_early_late_20_min'] = attendance_df['penalty_early_late_20_min'].apply(
            lambda item: item[0] if isinstance(item, list) else 0)
        attendance_df['penalty'] = attendance_df['penalty'].apply(
            lambda item: item if isinstance(item, int) else 0)

        for col, col_type in attendance_col_types.items():
            if col_type == 'datetime64[ns, UTC]':
                attendance_df[col] = attendance_df[col].apply(
                    lambda item: datetime.fromtimestamp(int(item / 1000)) if type(item) in (
                        float, int) and not math.isnan(
                        item) else None)
                attendance_df[col] = pd.to_datetime(attendance_df[col], utc=True)
            else:
                attendance_df[col] = attendance_df[col].astype(col_type)
        attendance_df = attendance_df[attendance_col_types.keys()]

        return {
            'employee': DataSet(
                name='employee',
                curr_data=employee_df,
                primary_keys=['user_id', 'partition_date'],
                storage_path='',
                table_name='employee',
                data_type='bigquery',
                database='bronze',
                partition=partition,
                replace_partition=True,
            ),
            'attendance_record': DataSet(
                name='attendance_record',
                curr_data=attendance_record_df,
                primary_keys=['record_id', 'partition_date'],
                storage_path='',
                table_name='attendance_record',
                data_type='bigquery',
                database='bronze',
                partition=partition,
                replace_partition=True,
            ),
            'attendance': DataSet(
                name='attendance',
                curr_data=attendance_df,
                primary_keys=['attendance_id', 'partition_date'],
                storage_path='',
                table_name='attendance',
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

        # get only latest customer rows in dim_customer
        # since dim customer may have multiple rows per customer (SCD2)
        dim_employee_latest = kwargs['dim_employee']

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

        # Assign dtype
        dim_employee_col_types = {
            'user_id': 'string',
            'lark_id': 'string',
            'leader_lark_id': 'string',
            'employee_sur_id': 'string',
            'name': 'string',
            'gender': 'string',
            'city': 'string',
            'email': 'string',
            'mobile': 'string',
            'job_title': 'string',
            'employee_type': 'string',
            'join_time': 'datetime64[ns, UTC]',
            'employee_no': 'string',
            'department_ids': 'object',
            'datetime_created': 'datetime64[ns, UTC]',
            'datetime_updated': 'datetime64[ns, UTC]',
            'is_current': 'bool',
            'valid_from': 'datetime64[ns, UTC]',
            'valid_to': 'datetime64[ns, UTC]'
        }
        for col, col_type in dim_employee_col_types.items():
            if col_type == 'datetime64[ns, UTC]':
                dim_employee_df[col] = pd.to_datetime(dim_employee_df[col], utc=True)
            else:
                dim_employee_df[col] = dim_employee_df[col].astype(col_type)

        return dim_employee_df

    def get_fact_attendance_record(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> DataFrame:
        dim_employee = input_datasets['dim_employee'].curr_data
        attendance_record_df = input_datasets['attendance_record'].curr_data
        fact_attendance_record_df = pd.merge(attendance_record_df, dim_employee, how='left', on=['user_id'],
                                             suffixes=['', '_right'])
        selected_cols = [col for col in attendance_record_df.columns if
                         col not in ['etl_inserted', 'partition_date']] + ['employee_sur_id']
        fact_attendance_record_df = fact_attendance_record_df[selected_cols]

        return fact_attendance_record_df

    def get_fact_attendance(
            self,
            input_datasets: Dict[str, DataSet],
            **kwargs,
    ) -> DataFrame:
        dim_employee = input_datasets['dim_employee'].curr_data
        attendance_df = input_datasets['attendance'].curr_data
        fact_attendance_df = pd.merge(attendance_df, dim_employee, how='left', on=['user_id'],
                                      suffixes=['', '_right'])
        selected_cols = [col for col in attendance_df.columns if
                         col not in ['lark_id', 'etl_inserted', 'partition_date']] + ['employee_sur_id']
        fact_attendance_df = fact_attendance_df[selected_cols]

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

        fact_attendance_df['attendance_date'] = pd.to_datetime(fact_attendance_df['attendance_date'])
        dim_employee_df['is_current'] = dim_employee_df['is_current'].astype(bool)

        dim_employee_current_df = dim_employee_df[dim_employee_df['is_current'] == True]

        merged_df = fact_attendance_df.merge(dim_employee_current_df, left_on='user_id', right_on='user_id',
                                             how='left')

        cube_attendance_report_df = pd.DataFrame()

        cube_attendance_report_df['attendance_month'] = merged_df['attendance_date'].dt.strftime('%Y-%m')
        cube_attendance_report_df['attendance_date'] = merged_df['attendance_date'].dt.strftime('%Y-%m-%d')
        cube_attendance_report_df['lark_hrm_code'] = merged_df['user_id']
        cube_attendance_report_df['hrm_name'] = merged_df['name']
        cube_attendance_report_df['job_title'] = merged_df['job_title']

        datetime_col = ['datetime_check_in', 'datetime_check_out', 'check_out_shift_time', 'check_in_shift_time']
        merged_df[datetime_col] = merged_df[datetime_col].apply(
            lambda x: pd.to_datetime(x, format='%d/%m/%Y %H:%M', errors='coerce'))

        cube_attendance_report_df['late_time_minute'] = (
                (merged_df['datetime_check_in'] + pd.Timedelta(hours=7) - merged_df[
                    'check_in_shift_time']).dt.total_seconds() / 60
        ).clip(upper=0).abs().fillna(0)

        cube_attendance_report_df['early_time_minute'] = (
                (merged_df['datetime_check_out'] + pd.Timedelta(hours=7) - merged_df[
                    'check_out_shift_time']).dt.total_seconds() / 60
        ).clip(upper=0).abs().fillna(0)

        cube_attendance_report_df['working_duration_hours'] = (
                (merged_df['datetime_check_out'] - merged_df['datetime_check_in']).dt.total_seconds() / 3600
        ).fillna(0)

        cube_attendance_report_df['working_duration_benchmark'] = (
                (merged_df['check_out_shift_time'] - merged_df['check_in_shift_time']).dt.total_seconds() / 3600
        ).fillna(0)

        cube_attendance_report_df['penalty_amount'] = merged_df['penalty']

        col_dtypes = {
            'attendance_month': 'string',
            'attendance_date': 'datetime64[us, UTC]',
            'lark_hrm_code': 'string',
            'hrm_name': 'string',
            'job_title': 'string',
            'late_time_minute': 'int64',
            'early_time_minute': 'int64',
            'working_duration_hours': 'int64',
            'working_duration_benchmark': 'int64',
            'penalty_amount': 'int64'
        }
        for col, col_type in col_dtypes.items():
            if col_type == 'datetime64[ns, UTC]':
                cube_attendance_report_df[col] = pd.to_datetime(cube_attendance_report_df[col], utc=True)
            else:
                cube_attendance_report_df[col] = cube_attendance_report_df[col].astype(col_type)

        cube_attendance_report_df.dropna(how='all', inplace=True)
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
