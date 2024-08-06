from airflow.providers.telegram.hooks.telegram import TelegramHook
import os
from dotenv import load_dotenv, find_dotenv

def send_telegram_success_message(context): 
    
    load_dotenv()
    telegram_token = os.environ.get('TELEGRAM_TOKEN')
    telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    print(telegram_token, telegram_chat_id)
    hook = TelegramHook(telegram_conn_id='test',
                        token=telegram_token,
                        chat_id=telegram_chat_id)
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} c id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })

    print(message)

def send_telegram_failure_message(context):
    
    load_dotenv()
    telegram_token = os.environ.get('TELEGRAM_TOKEN')
    telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    hook = TelegramHook(telegram_conn_id='test',
                        token=telegram_token,
                        chat_id=telegram_chat_id)
    dag = context['dag']
    tiks = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Не исполнение DAG {dag} с id={run_id}. Косяки: {tiks}'
    hook.send_message({
        'chat_id': telegram_chat_id,
        'text': message
    })
    print(message)

'''
send_telegram_success_message({
    'dag':'airflow',
    'task_instance_key_str': 'error :(',
    'run_id':'13456'
    })
'''