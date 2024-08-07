import sys
sys.path.append('/home/mle-user/mle_projects/mle_airflow/plugins/')

from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.clean import create_table, extract, transform, load

with DAG(
        dag_id='alt_clean_data_churn',
        schedule='@once',
        on_success_callback=send_telegram_success_message,
        on_failure_callback=send_telegram_failure_message,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        tags=["ALT_ETL_CLEAN_DATA","DAG_ALT_CLAEN_DATA"]
        ) as dag:
    
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step      = PythonOperator(task_id='extract'     , python_callable=extract)
    transform_step    = PythonOperator(task_id='transform'   , python_callable=transform)
    load_step         = PythonOperator(task_id='load'        , python_callable=load) 

    [extract_step,create_table_step] >> transform_step
    transform_step >> load_step