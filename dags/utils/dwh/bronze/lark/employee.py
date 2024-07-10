# CREATE TABLE bronze.lark_employee (
#     user_id STRING,
#     employee_no STRING,
#     name STRING,
#     user STRING,
#     employee_type STRING,
#     email STRING,
#     mobile STRING,
#     department_ids STRING,
#     departments STRING,
#     leader  STRING,
#     join_time TIMESTAMP,
#     job_title STRING,
#     city STRING,
#     gender STRING,
#     parent_items  STRING,
#     created_by  STRING,
#     modified_by STRING,
#     datetime_created TIMESTAMP,
#     datetime_updated TIMESTAMP,
#     etl_inserted TIMESTAMP,
#     partition_value DATE
# ) partition by partition_value;

TBL_EMPLOYEE = {
    'user_id': 'string',
    'employee_no': 'string',
    'name': 'string',
    'user': 'string',
    'employee_type': 'string',
    'email': 'string',
    'mobile': 'string',
    'department_ids': 'string',
    'departments': 'string',
    'leader': 'string',
    'join_time': 'datetime64[ns, UTC]',
    'job_title': 'string',
    'city': 'string',
    'gender': 'string',
    'Parent items': 'string',
    'Created By': 'string',
    'Modified By': 'string',
    'Date Created': 'datetime64[ns, UTC]',
    'Last Modified Date': 'datetime64[ns, UTC]'
}

RENAME_EMPLOYEE_COLS = {
    'Parent items': 'parent_items',
    'Created By': 'created_by',
    'Modified By': 'modified_by',
    'Date Created': 'datetime_created',
    'Last Modified Date': 'datetime_updated'
}