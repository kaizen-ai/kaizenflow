# This is a utility DAG to archive real time data
# The DAG fetches data older than a specified timestamp threshold
# from the specified table(s) and archives to the specified S3
# location(s). Once the data is archived, it's dropped from the DB table

# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval`` parameter.

# This DAG's configuration deploys tasks to AWS Fargate to offload the EC2s
# mainly utilized for rt download
import datetime
import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import airflow_utils.telegram.operator as aiutteop
import airflow_utils.aws.connection as aiutawco
import copy
import os

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to 
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]
_REGION = aiutecop.ASIA_REGION if components_dict["location"] == "tokyo" else aiutecop._EUROPE_REGION


# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""


_DAG_ID = components_dict["dag_id"]
# Base name of the db tables to archive, stage will be appended later. 
_DATASETS = [
    "periodic_daily.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0",
    "periodic_daily.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.binance.binance.v1_0_0",
    "periodic_daily.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0"
]
# If _DRY_RUN = True the data is not actually archived/deleted.
_DRY_RUN = False
_DAG_DESCRIPTION = f"Realtime DB data archival to S3"
_SCHEDULE = "0 */2 * * *"
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)

_BID_ASK_DATA_RETENTION_HOURS = 36

# Temporary hack to append tokyo to the path.
s3_db_archival_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/tokyo"

# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
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
    start_date=datetime.datetime(2023, 11, 30, 10, 00, 0),
    user_defined_macros = {
        "bid_ask_raw_data_retention_hours": _BID_ASK_DATA_RETENTION_HOURS,
    }, 
    tags=[_STAGE],
)
    
archival_command = [
   "/app/amp/im_v2/ccxt/db/archive_db_data_to_s3.py",
   f"--db_stage '{_STAGE}'",
   "--start_timestamp '{}'",
   "--end_timestamp '{}'",
   "--dataset_signature '{}'",
   f"--s3_path '{s3_db_archival_data_path}'",
   "--mode {}"
]

start_archival = DummyOperator(task_id='start_archival', dag=dag)
end_archival = DummyOperator(task_id='end_archival', dag=dag)
end_dag = DummyOperator(task_id='end_dag', dag=dag)
#check_previous_dag_run_state = aiutmisc.get_check_previous_run_operator(dag)

prev_archiving_task = None
prev_deleting_task = None

for dataset in _DATASETS:
    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(archival_command)
    curr_bash_command[2] = curr_bash_command[2].format("{{ data_interval_start }}")
    curr_bash_command[3] = curr_bash_command[3].format("{{ data_interval_end }}")
    curr_bash_command[4] = curr_bash_command[4].format(dataset)
    curr_bash_command[-1] = curr_bash_command[-1].format("archive_only")
    if _DRY_RUN:
        curr_bash_command.append("--dry_run")

    curr_archiving_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"archive_{dataset}",
        curr_bash_command,
        _ECS_TASK_DEFINITION, 
        4096, 
        30720,
        region=_REGION
    )
    
    curr_bash_command = copy.deepcopy(archival_command)
    curr_bash_command[2] = curr_bash_command[2].format("{{ data_interval_start - macros.timedelta(hours=bid_ask_raw_data_retention_hours) }}")
    curr_bash_command[3] = curr_bash_command[3].format("{{ data_interval_end - macros.timedelta(hours=bid_ask_raw_data_retention_hours) }}")
    curr_bash_command[4] = curr_bash_command[4].format(dataset)
    curr_bash_command[-1] = curr_bash_command[-1].format("delete_only")
    if _DRY_RUN:
        curr_bash_command.append("--dry_run")
        
    curr_deleting_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"delete_{dataset}",
        curr_bash_command,
        _ECS_TASK_DEFINITION, 
        512, 
        1024,
        region=_REGION
    )
        
    # We chain the archival and deletion operation instead of running in parallel
    # because they are HW intensive
    if prev_deleting_task:
        prev_deleting_task >> curr_archiving_task >> curr_deleting_task
    else:
        start_archival >> curr_archiving_task >> curr_deleting_task
    prev_deleting_task = curr_deleting_task
    
curr_deleting_task >> end_archival >> end_dag
    
if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(dag, _STAGE, "datapull", _DAG_ID, "{{ run_id }}")
    end_archival >> telegram_notification_task >> end_dag