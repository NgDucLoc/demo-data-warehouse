# CREATE TABLE silver.fact_attendance_record (
#   user_id STRING,
#   attendance_record_id STRING,
#   check_time TIMESTAMP,
#   check_location_name STRING,
#   is_offsite BOOL,
#   attendance_date TIMESTAMP,
#   etl_inserted TIMESTAMP,
#   partition_value DATE
# ) partition by partition_value;
