# This is a utility DAG to conduct QA on real time data download
# The task compares the downloaded data with the contents of
# of the database, to confirm a match or show discrepancies.

# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval`` parameter.

# This DAG's configuration deploys tasks to AWS Fargate to offload the EC2s
# mainly utilized for rt download
import datetime
import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from itertools import product
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
# List of dicts to specify parameters for each reconciliation jobs.
_RECONCILIATION_JOBS = [
    {
        "data_type": "ohlcv",
        "contract_type": "futures",
        "db_table_base_name": "ccxt_ohlcv_futures",
        "s3_vendor": "ccxt",
        "add_params": []
    },
    {
        "data_type": "ohlcv",
        "contract_type": "spot",
        "db_table_base_name": "ccxt_ohlcv",
        "s3_vendor": "ccxt",
        "add_params": []
    },
    {
        "data_type": "bid_ask",
        "contract_type": "futures",
        "db_table_base_name": "ccxt_bid_ask_futures_resampled_1min",
        "s3_vendor": "crypto_chassis",
        "add_params": ["--resample_1min"]
    },
]
_DAG_DESCRIPTION = "Daily data reconciliation"
_SCHEDULE = Variable.get(f"{_DAG_ID}_schedule")

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
s3_daily_staged_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_daily_staged_data_folder')}"

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
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
)
s3_daily_staged_data_path = s3_daily_staged_data_path.rstrip("/")
compare_command = [
    "/app/amp/im_v2/ccxt/data/extract/compare_realtime_and_historical.py",
    "--end_timestamp '{{ data_interval_end.replace(hour=0, minute=0, second=0) - macros.timedelta(minutes=1) }}'",
    "--start_timestamp '{{ data_interval_start.replace(hour=0, minute=0, second=0) }}'",
    "--aws_profile 'ck'",
    "--exchange_id 'binance'",
    "--db_stage 'dev'",
    "--db_table '{}'",
    "--data_type '{}'",
    "--contract_type '{}'",
    "--s3_vendor '{}'",
    f"--s3_path '{s3_daily_staged_data_path}'"
]

start_comparison = DummyOperator(task_id='start_comparison', dag=dag)
end_comparison = DummyOperator(task_id='end_comparison', dag=dag)

for job in _RECONCILIATION_JOBS:

    db_table = job["db_table_base_name"]
    db_table += f"_{_STAGE}" if _STAGE in ["test", "preprod"] else ""
    data_type, contract_type, s3_vendor = (
        job["data_type"],
        job["contract_type"],
        job["s3_vendor"]
    )

    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(compare_command)
    curr_bash_command[6] = curr_bash_command[6].format(db_table)
    curr_bash_command[7] = curr_bash_command[7].format(data_type)
    curr_bash_command[8] = curr_bash_command[8].format(contract_type)
    curr_bash_command[9] = curr_bash_command[9].format(s3_vendor)

    for param in job["add_params"]:
        curr_bash_command.append(param)

    kwargs = {}
    kwargs["network_configuration"] = {
        "awsvpcConfiguration": {
            "securityGroups": ecs_security_group,
            "subnets": ecs_subnets,
        },
    }

    comparing_task = ECSOperator(
        task_id=f"compare_ccxt_rt_{data_type}_{contract_type}_with_{s3_vendor}_daily",
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
            "cpu": "512",
            "memory": "2048"
        },
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
        execution_timeout=datetime.timedelta(minutes=15),
        **kwargs
    )

    start_comparison >> comparing_task >> end_comparison