"""
DAG to download OHLCV data from CoinGecko
"""

import datetime

import airflow
from airflow.operators.bash import BashOperator

_DAG_ID = "download_periodic_1min_postgres_coingecko"
_DAG_DESCRIPTION = "Download Coingecko data every day and save to Postgres"
# Specify when to execute the DAG.
_SCHEDULE = "*/2 * * * *"

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
    start_date=datetime.datetime(2023, 4, 15, 0, 0, 0),
)

bash_command = [
    # Sleep 5 seconds to ensure the bar is finished.
    "sleep 5",
    "&&",
    "/cmamp/sorrentum_sandbox/examples/ml_projects/Issue29_Team10_Implement_sandbox_for_coingecko/dowonload_to_db.py",
    "--target_table 'coingecko_historic'",
    "--id {{ data_id }} ",
    "--from_timestamp {{ data_interval_start }} ",
    "--to_timestamp {{ data_interval_end }}",
    "-v DEBUG",
]

downloading_task = BashOperator(
    task_id="download.periodic_1min.postgres.ohlcv.coingecko",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task
