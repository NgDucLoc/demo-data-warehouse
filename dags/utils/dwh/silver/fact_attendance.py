# CREATE TABLE silver.fact_attendance (
#   attendance_id STRING,
#   attendance_date DATE,
#   user_id STRING,
#   employee_sur_id STRING,
#   employee_type STRING,
#   group_name STRING,
#   shift_name STRING,
#   check_in_record_id STRING,
#   check_in_shift_time TIMESTAMP,
#   datetime_check_in TIMESTAMP,
#   check_in_location_name STRING,
#   check_in_is_offsite BOOL,
#   check_in_type STRING,
#   check_in_result STRING,
#   check_in_result_supplement STRING,
#   check_out_record_id STRING,
#   check_out_shift_time TIMESTAMP,
#   datetime_check_out TIMESTAMP,
#   check_out_location_name STRING,
#   check_out_is_offsite BOOL,
#   check_out_type STRING,
#   check_out_result STRING,
#   check_out_result_supplement STRING,
#   request_penalty BOOL,
#   early_late BOOL,
#   early_late_20_min BOOL,
#   penalty_early_late_20_min INT64,
#   penalty INT64,
#   reason STRING,
#   etl_inserted TIMESTAMP,
#   partition_value DATE
# ) partition by partition_value;