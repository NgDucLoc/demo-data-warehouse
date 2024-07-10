BGQ_TO_PD_DTYPE_DICTIONARY = {
    'TIMESTAMP': 'datetime64[ns, UTC]',
    'DATE': 'datetime64[ns, UTC]',
    'BOOLEAN': 'bool',
    'INTEGER': 'int64',
    'STRING': 'string'
}


def get_gbq_table_schema(client_gbq, dataset_name, table_name):
    dwh_table_ref = client_gbq.dataset(dataset_name).table(table_name)
    dwh_table = client_gbq.get_table(dwh_table_ref)
    dwh_schema = dwh_table.schema

    return dwh_schema


def map_gbq_to_pd_dtype(table_schema):
    col_dtypes = {}

    for column in table_schema:
        col_name = column.name
        col_type = column.field_type
        col_mode = column.mode

        if col_name in ['etl_inserted', 'partition_value']:
            continue

        if col_mode == 'REPEATED':
            col_dtypes[col_name] = 'object'
        elif col_type in BGQ_TO_PD_DTYPE_DICTIONARY.keys():
            col_dtypes[col_name] = BGQ_TO_PD_DTYPE_DICTIONARY[col_type]
        else:
            col_dtypes[col_name] = 'object'

    return col_dtypes


def apply_schema_to_df(client_gbq, dataset_name, table_name, data_df):
    table_schema = get_gbq_table_schema(
        client_gbq=client_gbq,
        dataset_name=dataset_name,
        table_name=table_name
    )
    col_dtypes = map_gbq_to_pd_dtype(table_schema)
    data_df = data_df[col_dtypes.keys()]
    data_df = data_df.astype(col_dtypes)

    return data_df
