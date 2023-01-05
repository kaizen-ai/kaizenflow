"""
Example DAG to download OHLCV data.

Import as:

import devops.surrentum_data_node.airflow_data.dags.download_periodic_1min_postgres_ohlcv as dsdnadddp1po
"""

import datetime

import airflow
from airflow.operators.bash import BashOperator

_DAG_ID = "download_periodic_1min_postgres_ohlcv"
_DAG_DESCRIPTION = "Download binance OHLCV data every minute and save to postgres"
# Specify when/how often to execute the DAG.
_SCHEDULE = "* * * * *"

# Pass default parameters for the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2022, 12, 23, 0, 0, 0),
)

bash_command = [
    # Sleep 5 seconds to ensure the bar is finished
    "sleep 5",
    "&&",
    "/cmamp/surrentum_infra_sandbox/examples/binance/download_to_db.py",
    "--target_table 'binance_ohlcv_spot_downloaded_1min'",
    "--start_timestamp {{ data_interval_start }} ",
    "--end_timestamp {{ data_interval_end }}",
]

downloading_task = BashOperator(
    task_id="download.periodic_1min.postgres.ohlcv.binance",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task