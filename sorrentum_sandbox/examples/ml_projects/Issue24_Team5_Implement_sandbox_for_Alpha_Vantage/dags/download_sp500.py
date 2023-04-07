"""
DAG to download stock market data.
"""


import datetime
import os

import airflow
from airflow.operators.bash_operator import BashOperator

from dotenv import load_dotenv

load_dotenv()

_DAG_ID = "download_sp500_alpha_vantage"
_DAG_DESCRIPTION = (
    "Download tickers every day and save to MongoDB"
)
# Specify when to execute the DAG.
_SCHEDULE = "0 0 * * *"

# Default parameters for the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2022, 1, 1),
    "end_date": datetime.datetime(2023, 4, 3),
    "email": [os.environ.get("EMAIL")],
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
    "python /cmamp/update_sp500.py"
    # , "--start_timestamp {{ data_interval_start }}",
    # "--end_timestamp {{ data_interval_end }}",
    # "-v DEBUG"
]

downloading_task = BashOperator(
    task_id="download.sp500.alpha_vantage",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task
