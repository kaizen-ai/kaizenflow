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
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import copy
import os

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to 
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
_STAGE = _FILENAME.split(".")[0]
assert _STAGE in ["prod", "preprod", "test"]

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# Deployment type, if the task should be run via fargate (serverless execution)
# or EC2 (machines deployed in our auto-scaling group)
_LAUNCH_TYPE = "fargate"
assert _LAUNCH_TYPE in ["ec2", "fargate"]

_DAG_ID = _FILENAME.rsplit(".", 1)[0]
# Base name of the db tables to archive, stage will be appended later. 
_DB_TABLES = ["ccxt_bid_ask_futures_raw"]
# If _DRY_RUN = True the data is not actually archived/deleted.
_DRY_RUN = False
_DAG_DESCRIPTION = f"Realtime data archival to S3 of table(s):" \
                + f"{_DB_TABLES}."
_SCHEDULE = Variable.get(f'{_DAG_ID}_schedule')

# Used for container overrides inside DAG task definition.
# If this is a test DAG don't forget to add your username to container suffix.
# i.e. cmamp-test-juraj since we try to follow the convention of container having
# the same name as task-definition if applicable
# Set to the name your task definition is suffixed with i.e. cmamp-test-juraj,
_CONTAINER_SUFFIX = f"-{_STAGE}" if _STAGE in ["preprod", "test"] else ""
_CONTAINER_SUFFIX += f"-{_USERNAME}" if _STAGE == "test" else ""
_CONTAINER_NAME = f"cmamp{_CONTAINER_SUFFIX}"

ecs_cluster = Variable.get(f'{_STAGE}_ecs_cluster')
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize 
# convolution and maximize simplicity.
ecs_task_definition = _CONTAINER_NAME

# Subnets and security group is not needed for EC2 deployment but 
# we keep the configuration header unified for convenience/reusability.
ecs_subnets = [Variable.get("ecs_subnet1")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
s3_db_archival_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_db_archival_path')}"

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
    catchup=True,
    start_date=datetime.datetime(2022, 12, 17, 21, 0, 0),
)
    
archival_command = [
   "/app/amp/im_v2/ccxt/db/archive_db_data_to_s3.py",
   "--db_stage 'dev'",
   "--timestamp '{{ data_interval_end - macros.timedelta(hours=var.value.db_archival_delay_hours | int) }}'",
   "--db_table '{}'",
   f"--s3_path '{s3_db_archival_data_path}'",
   # The command needs to be executed manually first because --incremental 
   # assumes appending to existing folder.
   "--incremental"
]

start_archival = DummyOperator(task_id='start_archival', dag=dag)
end_archival = DummyOperator(task_id='end_archival', dag=dag)

for db_table in _DB_TABLES:

    db_table_with_stage = db_table
    db_table_with_stage += f"_{_STAGE}" if _STAGE in ["test", "preprod"] else ""
    
    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(archival_command)
    curr_bash_command[3] = curr_bash_command[3].format(db_table_with_stage)
    if _DRY_RUN:
        curr_bash_command.append("--dry_run")

    kwargs = {}
    kwargs["network_configuration"] = {
        "awsvpcConfiguration": {
            "securityGroups": ecs_security_group,
            "subnets": ecs_subnets,
        },
    }
    
    archiving_task = ECSOperator(
        task_id=f"archive_{db_table_with_stage}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type=_LAUNCH_TYPE.upper(),
        overrides={
            "containerOverrides": [
                {
                    "name": _CONTAINER_NAME,
                    "command": curr_bash_command,
                }
            ],
            "cpu": "2048",
            "memory": "10240"
        },
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
        execution_timeout=datetime.timedelta(minutes=30),
        **kwargs
    )
    
    start_archival >> archiving_task >> end_archival