import pandas as pd
from dags.utils.bigquery import (get_data_from_gcs,get_files_recent_partition,rename_columns,upsert_to_bigquery)

def main():
    # 1. Extract
    bucket_name = 'datalize_raw'
    area = 'raw'
    model_name = 'attendance_record'
    model_type = 'current'

    # Giới hạn dữ liệu bằng biz logic
    file_names = get_files_recent_partition(model_name, 2, type_of_time="months", file_extension=".csv")

    extract_df = get_data_from_gcs(bucket_name, area, model_name, model_type, file_names)

    # 2. Transform
    # Trim
    trim_df = extract_df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    # Rename
    rename_col_dict = {
        "Record id": "attendance_record_id",
        "User id": "user_id",
        "Check time": "check_time",
        "Check location name": "location",
        "Is offsite": "is_offsite",
        "Date": "attendance_date"
    }
    rename_df = rename_columns(trim_df, rename_col_dict)

    # Cast Type
    int_numeric_col = ['attendance_record_id']
    numeric_col = []
    string_col = ['user_id', 'location']
    bool_col = ['is_offsite']
    datetime_col = ['check_time']
    date_col = ['attendance_date']

    rename_df[int_numeric_col] = rename_df[int_numeric_col].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    rename_df[bool_col] = rename_df[bool_col].apply(lambda x: x.astype(bool))
    rename_df[date_col] = rename_df[date_col].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y', errors='coerce'))
    rename_df[datetime_col] = rename_df[datetime_col].apply(
        lambda x: pd.to_datetime(x, format='%d/%m/%Y %H:%M', errors='coerce'))

    # Add column
    rename_df['data_source'] = 'lark_attendance'

    # 3. Load
    upsert_to_bigquery(rename_df, 'fact_' + model_name, ['source_table', 'attendance_record_id'])


if __name__ == "__main__":
    main()