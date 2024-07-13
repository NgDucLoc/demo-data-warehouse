# CREATE TABLE bronze.lark_attendance_record (
#     user_id STRING,
#     attendance_record_id STRING,
#     attendance_date TIMESTAMP,
#     employee STRING,
#     check_time TIMESTAMP,
#     check_location_name STRING,
#     is_offsite BOOL,
#     etl_inserted TIMESTAMP,
#     partition_value DATE
# ) PARTITION BY partition_value;

TBL_ATTENDANCE_RECORD = {
    'User id': 'string',
    'Record id': 'string',
    'Date': 'datetime64[ns, UTC]',
    'Employee': 'string',
    'Check time': 'datetime64[ns, UTC]',
    'Check location name': 'string',
    'Is offsite': 'bool'
}

RENAME_ATTENDANCE_RECORD_COLS = {
    'User id': 'user_id',
    'Record id': 'attendance_record_id',
    'Date': 'attendance_date',
    'Employee': 'employee',
    'Check time': 'check_time',
    'Check location name': 'check_location_name',
    'Is offsite': 'is_offsite'
}
