from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import download_yahoo as sisebido
from Latest_Data import dump_latest_data


default_args = {
    'owner': 'coder2j',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


def dump_2m():
    dump_latest_data('yahoo_yfinance_spot_downloaded_2min','2m')


with DAG(
    default_args=default_args,
    dag_id="dag_2min",
    start_date=datetime(2023, 4, 15,22,35,0),
    schedule_interval='*/10 * * * *'
) as dag:
    task1 = PythonOperator(
        task_id='dump_2m',
        python_callable=dump_2m
    )
    
   
    task1 