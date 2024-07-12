# CREATE TABLE gold.cube_attendance_report (
#     attendance_month STRING,
#     attendance_date DATE,
#     lark_hrm_code STRING,
#     hrm_name STRING,
#     job_title STRING,
#     late_time_minute int64,
#     early_time_minute int64,
#     working_duration_hours int64,
#     working_duration_benchmark int64,
#     penalty_amount int64,
#     etl_inserted TIMESTAMP,
#     partition_value DATE,
# ) partition by partition_value;
