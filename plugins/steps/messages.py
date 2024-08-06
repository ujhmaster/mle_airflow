from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.models import Variable
from airflow.notifications.basenotifier import BaseNotifier


def send_telegram_success_message(context): 
    
    telegram_token   = Variable.get("telegram_token")
    telegram_chat_id = Variable.get("telegram_chat_id")
    
    hook = TelegramHook(telegram_conn_id='telegram_bot',
                        token=telegram_token,
                        chat_id=telegram_chat_id)
    dag = context['ti'].dag_id
    run_id = context['ti'].run_id
    
    message = f'Исполнение DAG {dag} c id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })

def send_telegram_failure_message(context):
    
    telegram_token   = Variable.get("telegram_token")
    telegram_chat_id = Variable.get("telegram_chat_id")

    hook = TelegramHook(telegram_conn_id='telegram_bot',
                        token=telegram_token,
                        chat_id=telegram_chat_id)
    
    dag = context['ti'].dag_id
    run_id = context['ti'].run_id
    tiks = context['ti'].log_url
    
    message = f'Не исполнение DAG {dag} с id={run_id}. Косяки: {tiks}'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })
