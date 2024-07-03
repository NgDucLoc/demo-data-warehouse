from google.cloud import storage, bigquery
from google.oauth2 import service_account


def get_bigquery_connection(service_account_json):
    credentials = service_account.Credentials.from_service_account_info(service_account_json)
    client_gcs = storage.Client(credentials=credentials)
    client_gbq = bigquery.Client(credentials=credentials)

    return client_gcs, client_gbq, credentials