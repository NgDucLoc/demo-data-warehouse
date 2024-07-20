import logging
import json
import requests

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


class Lark:
    def __init__(self, app_id, app_secret, group_chat_id=None, api_page_size=20):
        self.base_url = 'open.larksuite.com'
        self.app_id = app_id
        self.app_secret = app_secret
        self.api_page_size = api_page_size
        self.group_chat_id = group_chat_id
        self.tenant_access_token = self.get_tenant_access_token()

    def get_tenant_access_token(self):
        task_logger.info('[get_tenant_access_token] Getting tenant_access_token ...')

        try:
            api_url = f'https://{self.base_url}/open-apis/auth/v3/tenant_access_token/internal/'
            body = {
                'app_id': self.app_id,
                'app_secret': self.app_secret
            }
            response = requests.post(api_url, data=body)
            response_data = response.json()

            if response.status_code != 200 or response_data.get('code') != 0:
                task_logger.info(
                    f'[{response.status_code}] Error in get tenant_access_token, msg: {response.json().get("msg", "")}')
            return response_data.get('tenant_access_token', None)
        except Exception as e:
            task_logger.error(f'[get_tenant_access_token] Error function: {e}')

        return None

    def get_records(self, app_token, table_id, table_name):
        task_logger.info('[get_records] Fetch records from table...')

        records = []
        try:
            api_url = f'https://{self.base_url}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records'
            headers = {
                'Authorization': f'Bearer {self.tenant_access_token}',
            }
            page_token = None
            has_more = True
            while has_more:
                params = {
                    'page_size': self.api_page_size
                }
                if page_token:
                    params['page_token'] = page_token
                response = requests.get(api_url, headers=headers, params=params)
                response_data = response.json()

                if response.status_code != 200 or response_data.get('code') != 0:
                    task_logger.info(
                        f'[{response.status_code}] Error in fetch records from table {table_name}, msg: {response.json().get("msg", "")}')
                else:
                    records.extend(response_data.get('data', {}).get('items', []))
                has_more = response_data.get('data', {}).get('has_more', False)
                page_token = response_data.get('data', {}).get('page_token', None)
        except Exception as e:
            task_logger.error(f'[get_records] Error function: {e}')

        return records

    def get_tables(self, app_token):
        task_logger.info('[get_tables] Fetch tables...')

        tables = []
        try:
            if not app_token:
                task_logger.warning('Please add app_token for lark base to fetch data')
                return None

            if not self.tenant_access_token:
                return None

            # Get tables list
            api_url = f'https://{self.base_url}/open-apis/bitable/v1/apps/{app_token}/tables'
            headers = {
                'Authorization': f'Bearer {self.tenant_access_token}',
            }
            has_more = True
            page_token = None
            tables = []
            while has_more:
                params = {
                    'page_size': self.api_page_size
                }
                if page_token:
                    params['page_token'] = page_token
                response = requests.get(api_url, headers=headers, params=params)
                response_data = response.json()

                if response.status_code != 200 or response_data.get('code') != 0:
                    task_logger.info(
                        f'[{response.status_code}] Error in fetch tables from base {app_token}, msg: {response.json().get("msg", "")}')
                else:
                    tables.extend([{'table_id': item.get('table_id', None), 'name': item.get('name', None),
                                    'lark_app_token': app_token} for item in
                                   response_data.get('data', {}).get('items', [])])
                has_more = response_data.get('data', {}).get('has_more', False)
                page_token = response_data.get('data', {}).get('page_token', None)
        except Exception as e:
            task_logger.error(f'[get_tenant_access_token] Error function: {e}')

        return tables

    def send_message(self, message_body):
        task_logger.info('[send_message_to_group_chat] Sending message to group chat...')

        try:
            api_url = f'https://{self.base_url}/open-apis/im/v1/messages?receive_id_type=chat_id'
            headers = {
                'Authorization': f'Bearer {self.tenant_access_token}',
            }
            body = {
              "receive_id": self.group_chat_id,
              "msg_type": "text",
              "content": json.dumps({"text": message_body})
            }
            response = requests.post(api_url, headers=headers, data=body)
            response_data = response.json()

            if response.status_code != 200 or response_data.get('code') != 0:
                task_logger.info(
                    f'[{response.status_code}] Error in get send_message_to_group_chat, msg: {response.json().get("msg", "")}')
        except Exception as e:
            task_logger.error(f'[send_message_to_group_chat] Error function: {e}')
