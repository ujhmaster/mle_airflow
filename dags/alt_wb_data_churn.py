# dag scripts for World Bank Dataset
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from steps.wb_data import create_table, extract, transform, load
from datetime import timedelta
from steps.messages import send_telegram_success_message, send_telegram_failure_message
LOG_FORMAT  = f'WB_DATA DAG - '

with DAG(
    dag_id='prepare_wb_data_alt',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
        },
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["WorldBank", "ETL", "Test"]) as dag:

    step1 = PythonOperator(task_id='create_table', python_callable=create_table)
    step2 = PythonOperator(task_id='extract', python_callable=extract)
    step3 = PythonOperator(task_id='transform', python_callable=transform)
    step4 = PythonOperator(task_id='load', python_callable=load)

    # step1 >> step2 >> step3 >> step4
    chain([step1, step2], step3, step4)
