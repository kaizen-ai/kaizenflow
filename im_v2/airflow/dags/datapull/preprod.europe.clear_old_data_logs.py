"""
This is a utility DAG to cleanup airflow logs

General guide (many parts of the DAG can be reused but
some might need custom adjustment based on a particular dataset 
which needs downloading)

The DAG is annotated with enumerated comments explaining
what might need to be changed. i.e. 
# Guide 1.
"""

import datetime
import airflow
import airflow_utils.ecs.operator as aiutecop
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import airflow_utils.telegram.operator as aiutteop
import airflow_utils.misc as aiutmisc
from airflow.models import Variable
from itertools import product
import copy
import os

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to 
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
# Guide 1. Make sure to run the bulk downloaders in the test environment
#  to ensure non-interference with production system. 
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]
_REGION = aiutecop.ASIA_REGION if components_dict["location"] == "tokyo" else aiutecop._EUROPE_REGION

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
# Guide 2. This will determine which task-definition will be used.
#  As a dev you should have one dedicated to you -> cmamp-test-you.
_USERNAME = ""

# Specify the log prefixes to clear.
# Specify the log prefixes to clear.
# The preprod dag also clears the test stage.
_LOG_PREFIXES = ["test", "preprod"] if _STAGE in ["test", "preprod"] else ["prod"]

_DAG_ID = components_dict["dag_id"]
_DAG_DESCRIPTION = "Remove old logs"
# Guide 6. Set up an arbitrary interval to download the data in chunks.
_SCHEDULE = "0 6 * * *"

# Number of days to keep logs for.
_LOG_RETENTION_PERIOD_IN_DAYS = 7

# Log location
_LOG_BASE_DIRECTORY = "/opt/airflow/logs/"
_LOG_SCHEDULER_DIRECTORY = "/opt/airflow/logs/scheduler/"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f'{_STAGE}_notification_email')],
    "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
    "email_on_retry": False,
    "owner": "airflow",
}

# Create a DAG.
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2023, 4, 30, 0, 0, 0),
    tags=[_STAGE],
)

# Guide 8. Specify commands which need to be run.
clear_command = [
    "du -cksh {}{}{}* | tail -n 1",
    "&&",
    "find {}{}{}* -mindepth 2 -maxdepth 2 -mtime ",
    f"+{_LOG_RETENTION_PERIOD_IN_DAYS} -type d -exec ", 
    "rm -rf {} \\;",
    "&&",
    "du -cksh {}{}{}* | tail -n 1"
]

clear_scheduler_command = [
    "du -cksh {}* | tail -n 1",
    "&&",
    "find {}* -mindepth 1 -maxdepth 1 -mtime ",
    f"+{_LOG_RETENTION_PERIOD_IN_DAYS} -type d -exec ", 
    "rm -rf {} \\;",
    "&&",
    "du -cksh {}* | tail -n 1"
]

start_task = DummyOperator(task_id='start_dag', dag=dag)
end_task = DummyOperator(task_id='end_tasks', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Do a deepcopy of the bash command list so we can reformat params on each iteration.
clear_scheduler_command = copy.deepcopy(clear_scheduler_command)
clear_scheduler_command[0] = clear_scheduler_command[0].format(_LOG_BASE_DIRECTORY)
clear_scheduler_command[2] = clear_scheduler_command[2].format(_LOG_BASE_DIRECTORY)
clear_scheduler_command[6] = clear_scheduler_command[6].format(_LOG_SCHEDULER_DIRECTORY)
clear_log_scheduler_task = BashOperator(
    task_id = f"clear_logs_scheduler",
    bash_command = " ".join(clear_scheduler_command),
)
start_task >> clear_log_scheduler_task

for log_prefix in _LOG_PREFIXES:
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(clear_command)
    curr_bash_command[0] = curr_bash_command[0].format(_LOG_BASE_DIRECTORY, 'dag_id=', log_prefix)
    curr_bash_command[2] = curr_bash_command[2].format(_LOG_BASE_DIRECTORY, 'dag_id=', log_prefix)
    curr_bash_command[6] = curr_bash_command[6].format(_LOG_BASE_DIRECTORY, 'dag_id=', log_prefix)
    clear_log_task = BashOperator(
        task_id = f"clear_logs_{log_prefix}",
        bash_command = " ".join(curr_bash_command),
    )
    clear_log_scheduler_task >> clear_log_task >> end_task >> end_dag

if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(dag, _STAGE, "datapull", _DAG_ID, "{{ run_id }}")
    end_task >> telegram_notification_task >> end_dag