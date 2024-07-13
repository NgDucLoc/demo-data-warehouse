# create table bronze.lark_payment (
# 	payment_id STRING,
# 	payment_name STRING,
# 	payment_type STRING,
# 	payment_date DATE,
# 	project_name STRING,
# 	goods STRING,
# 	price_unit INT64,
# 	quantity INT64,
# 	price_total INT64,
# 	bill_image STRING,
# 	billing_evidence STRING,
# 	buying_person STRING,
# 	billing_person STRING,
# 	bank_acc_number STRING,
# 	bank_holder STRING,
# 	note STRING,
# 	was_approved_by_ceo BOOL,
# 	was_paid_by_accountant BOOL,
# 	was_recieve BOOL,
# 	ceo_approved_datetime TIMESTAMP,
# 	accountant_paid_datetime TIMESTAMP,
# 	recieve_datetime TIMESTAMP,
# 	etl_inserted TIMESTAMP,
# 	partition_value DATE
# ) partition by partition_value;

TBL_PAYMENT = {
    "Payment": "string",
    "Loại chi phí": "string",
    "Ngày mua": "datetime64[ns, UTC]",
    "Tên dự án": "string",
    "Hàng hóa": "string",
    "Đơn giá": "int64",
    "Số lượng": "int64",
    "Tổng tiền": "int64",
    "Hóa đơn": "string",
    "Minh chứng chuyển khoản": "string",
    "Thông tin người cần chuyển khoản": "string",
    "Số tài khoản": "string",
    "Ngân hàng": "string",
    "Người mua": "string",
    "Ghi chú": "string",
    "CEO duyệt": "bool",
    "Kế toán đã thanh toán": "bool",
    "Người mua đã nhận được": "bool",
    "Ngày CEO duyệt": "datetime64[ns, UTC]",
    "Ngày kế toán chuyển tiền": "datetime64[ns, UTC]",
    "Ngày người mua nhận tiền": "datetime64[ns, UTC]",
    "Payment_ID": "string"
}

RENAME_PAYMENT_COLS = {
    "Payment": "payment_name",
    "Loại chi phí": "payment_type",
    "Ngày mua": "payment_date",
    "Tên dự án": "project_name",
    "Hàng hóa": "goods",
    "Đơn giá": "price_unit",
    "Số lượng": "quantity",
    "Tổng tiền": "price_total",
    "Hóa đơn": "bill_image",
    "Minh chứng chuyển khoản": "billing_evidence",
    "Thông tin người cần chuyển khoản": "billing_person",
    "Số tài khoản": "bank_acc_number",
    "Ngân hàng": "bank_holder",
    "Người mua": "buying_person",
    "Ghi chú": "note",
    "CEO duyệt": "was_approved_by_ceo",
    "Kế toán đã thanh toán": "was_paid_by_accountant",
    "Người mua đã nhận được": "was_recieve",
    "Ngày CEO duyệt": "ceo_approved_datetime",
    "Ngày kế toán chuyển tiền": "accountant_paid_datetime",
    "Ngày người mua nhận tiền": "recieve_datetime",
    "Payment_ID": "payment_id"
}
