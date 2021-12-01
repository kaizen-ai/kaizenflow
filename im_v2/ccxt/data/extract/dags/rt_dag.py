import datetime

import airflow
import pandas as pd
from airflow.operators.bash import BashOperator

import helpers.datetime_ as hdateti

# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "email": ["d.tikhomirov@crypto-kaizen.com"],
    "email_on_failure": True,
    "owner": "test",
}

with airflow.DAG(
    dag_id="realtime_ccxt",
    description="Realtime download of CCXT OHLCV data",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    start_date=datetime.datetime.now(),
) as dag:
    # Pass default parameters for the script.
    script_args = {
        "dst_dir": "test/default_dir",
        "data_type": "ccxt_ohlcv",
        "universe": "v3",
        "api_keys": "API_keys.json",
        "table_name": "ccxt_ohlcv",
    }

    end_datetime = hdateti.get_timestamp("UTC")
    start_datetime = (
        pd.Timestamp(end_datetime) - pd.Timedelta("5 minutes")
    ).strftime("%Y%m%d-%H%M%S")

    # TODO(Danya): Rewrite as a collection of PythonOperators.
    bash_command = (
        f"python im_v2/ccxt/data/extract/download_realtime.py"
        f"--start_datetime {start_datetime}"
        f"--end_datetime {end_datetime}"
        f"--dst_dir {script_args['dst_dir']}"
        f"--data_type {script_args['data_type']}"
        f"--table_name {script_args['table_name']}"
        f"--api_keys {script_args['api_keys']}"
        f"--universe {script_args['universe']}"
    )

    downloading_task = BashOperator(
        task_id="run_script", bash_command=bash_command, default_args=default_args
    )
