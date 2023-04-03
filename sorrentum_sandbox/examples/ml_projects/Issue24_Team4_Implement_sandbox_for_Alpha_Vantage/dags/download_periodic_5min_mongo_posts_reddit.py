"""
DAG to download stock market data.
"""


import datetime

import airflow
from airflow.operators.bash_operator import BashOperator

_DAG_ID = "download_periodic_hourly_alpha_vantage_tickers"
_DAG_DESCRIPTION = (
    "Download tickers every hour and save to MongoDB"
)
# Specify when to execute the DAG.
_SCHEDULE = "0 * * * *"

# Default parameters for the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2023,4,2),
    "end_date":datetime.datetime(2023,4,3),
    "email": ["ajoshi18@umd.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    # start_date=datetime.datetime(2022, 1, 1, 0, 0, 0),
)

bash_command = [
    # Sleep 5 seconds to ensure the post is submitted.
    "sleep 5",
    "&&",
    "python /cmamp/download_to_db.py"
    #, "--start_timestamp {{ data_interval_start }}",
    # "--end_timestamp {{ data_interval_end }}",
    # "-v DEBUG"
]

downloading_task = BashOperator(
    task_id="download.airflow.downloaded.hourly.mongo.tickers.alpha_vantage",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task