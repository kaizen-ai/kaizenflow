"""
Example DAG to download reddit data.

Import as:

import devops.surrentum_data_node.airflow_data.dags.download_airflow_downloaded_5min_mongo_posts_reddit as dsdnaddad5mr
"""

import datetime

import airflow
from airflow.operators.bash import BashOperator

_DAG_ID = "download_airflow_downloaded_5min_mongo_posts_reddit"
_DAG_DESCRIPTION = "Download reddit posts every 5 minutes and save to MongoDB"
# Specify when/how often to execute the DAG.
_SCHEDULE = "*/5 * * * *"

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
    start_date=datetime.datetime(2022, 1, 1, 0, 0, 0),
)

bash_command = [
    # Sleep 5 seconds to ensure the post is submitted
    "sleep 5",
    "&&",
    "/cmamp/surrentum_infra_sandbox/examples/reddit/example_extract.py",
    "--start_timestamp {{ data_interval_start }}",
    "--end_timestamp {{ data_interval_end }}",
]

downloading_task = BashOperator(
    task_id="download.airflow.downloaded.5mi.mongo.posts.reddit",
    depends_on_past=False,
    bash_command=" ".join(bash_command),
    dag=dag,
)

downloading_task
