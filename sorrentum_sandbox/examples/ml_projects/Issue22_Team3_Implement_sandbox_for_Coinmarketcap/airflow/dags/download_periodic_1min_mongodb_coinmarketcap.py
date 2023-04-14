"""
DAG to download data from coinmarketcap to mongodb.
"""

import datetime

import airflow
from datetime import timedelta
from airflow.operators.bash import BashOperator

_DAG_ID = "download_periodic_1min_mongodb_coinmarketcap"
_DAG_DESCRIPTION = (
    "Download coinmarketcap data every minute and save to MongoDB."
)


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
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    start_date=datetime.datetime(2023, 4, 13, 0, 0, 0),
)

bash_command = [
    # Sleep 5 seconds to ensure the bar is finished.
    "sleep 5",
    "&&",
    "cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue22_Team3_Implement_sandbox_for_Coinmarketcap",
    "&&",
    "./download_to_db.py --start 1 --limit 200 --collection_name coinmarketcap_spot_downloaded_1min -v DEBUG",
]

downloading_task = BashOperator(
    task_id="download.periodic_1min.mongodb.coinmarketcap",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task