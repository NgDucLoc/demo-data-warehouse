from airflow.notifications.basenotifier import BaseNotifier
from airflow.models import Variable
from utils.lark import Lark


class LarkChatNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message):
        self.message = message
        self.lark = Lark(
            app_id=Variable.get('lark_app_id', default_var=None),
            app_secret=Variable.get('lark_app_secret', default_var=None),
            group_chat_id=Variable.get('lark_group_chat_id', default_var=None)
        )

    def notify(self, context):
        self.lark.send_message(self.message)
