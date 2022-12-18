"""
Download 1m interval historical OHLCV data
every 5 minutes
from the Binance
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG.
with DAG(
    dag_id="OHLCV_5_MIN_HISTORICAL_DOWNLOAD",
    description="Download 1m interval historical OHLCV data",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    start_date=datetime(2021, 1, 1),
) as dag:
    start_task = DummyOperator(task_id='start_dag', dag=dag)
    end_download = DummyOperator(task_id='end_dag', dag=dag)
    bash_command = (
        "cd /opt/airflow/cmamp && "
        "surrentum_infra_sandbox/"
        "download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0.py"
        "  "
        "--output_file test1.csv "
        "--start_timestamp '2022-10-20 10:00:00-04:00' "
        "--end_timestamp '2022-10-20 10:05:00-04:00' "
    )
    downloading_task = BashOperator(
        task_id='download_ohlcv_5_minutes',
        depends_on_past=False,
        bash_command=bash_command,
    )
    start_task >> downloading_task >> end_download
