import datetime

import airflow
from airflow.operators.bash import DockerOperator

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
        "period_length": "5 minutes",
    }
    # Build a bash command to execute.
    bash_command = (
        f"python im_v2/ccxt/data/extract/download_realtime.py"
        # Provide end of the time period as UTC timestamp.
        f"--end_datetime {datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')} "
        f"--period_length {script_args['period_length']} "
        f"--dst_dir {script_args['dst_dir']} "
        f"--data_type {script_args['data_type']} "
        f"--table_name {script_args['table_name']} "
        f"--api_keys {script_args['api_keys']} "
        f"--universe {script_args['universe']}"
    )
    # Run the script.
    downloading_task = DockerOperator(
        task_id="run_script",
        image="665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev",
        bash_command=bash_command,
        default_args=default_args,
    )
