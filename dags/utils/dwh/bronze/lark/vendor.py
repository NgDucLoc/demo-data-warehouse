# CREATE TABLE bronze.lark_vendor (
#     vendor_id STRING,
#     bank_holder_name STRING,
#     bank_acc_number STRING,
#     bank_holder STRING,
#     qr_code STRING,
#     note STRING,
#     etl_inserted TIMESTAMP,
#     partition_value DATE
# ) PARTITION BY partition_value;

TBL_VENDOR = {
    'Vendor': 'string',
    'Tên tài khoản': 'string',
    'Số tài khoản': 'string',
    'Ngân hàng': 'string',
    'QR code': 'string',
    'Ghi chú': 'string',
    'Date Created': 'datetime64[ns, UTC]',
    'Last Modified Date': 'datetime64[ns, UTC]'
}

RENAME_VENDOR_COLS = {
    'Vendor': 'vendor_id',
    'Tên tài khoản': 'bank_holder_name',
    'Số tài khoản': 'bank_acc_number',
    'Ngân hàng': 'bank_holder',
    'QR code': 'qr_code',
    'Ghi chú': 'note',
    'Date Created': 'datetime_created',
    'Last Modified Date': 'datetime_updated'
}
