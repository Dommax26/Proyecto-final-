from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Un DAG para ejecutar el pipeline ETL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def run_etl_script():
    try:
        script_path = r"C:\Users\Acer\PycharmProjects\Proyecto Final\etl_pipeline.py"
        os.system(f"python {script_path}")
    except Exception as e:
        logging.error(f"Error al ejecutar el script ETL: {e}")
        raise

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl_script,
    dag=dag,
)

etl_task
