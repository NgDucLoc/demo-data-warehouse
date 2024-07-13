# CREATE TABLE silver.dim_vendor (
#     vendor_id STRING,
#     vendor_sur_id STRING,
#     bank_holder_name STRING,
#     bank_acc_number STRING,
#     bank_holder STRING,
#     qr_code STRING,
#     note STRING,
#     datetime_created TIMESTAMP,
#     datetime_updated TIMESTAMP,
#     valid_from TIMESTAMP,
#     valid_to TIMESTAMP,
#     is_current BOOL,
#     etl_inserted TIMESTAMP,
#     partition_value DATE
# ) PARTITION BY partition_value;
