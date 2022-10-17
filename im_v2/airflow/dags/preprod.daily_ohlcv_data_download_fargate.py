# This is a utility DAG to download OHLCV data daily.

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
_EXCHANGES = ["binance"]
_PROVIDERS = ["crypto_chassis", "ccxt"]
_UNIVERSES = { "crypto_chassis": "v2", "ccxt" : "v7"}
_CONTRACTS = ["spot", "futures"]
_DATA_TYPES = ["ohlcv"]
_DAG_DESCRIPTION = f"Daily {_DATA_TYPES} data download, contracts:" \
                + f"{_CONTRACTS}, using {_PROVIDERS} from {_EXCHANGES}."
# Specify when/how often to execute the DAG.
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
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"
s3_daily_staged_data_path = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_daily_staged_ohlcv_data_folder')}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 1,
    # CryptoChassis might throw
    # too many request error, wait a couple min to retry.
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

download_command = [
    "/app/amp/im_v2/{}/data/extract/download_historical_data.py",
     "--end_timestamp '{{ execution_date + macros.timedelta(hours=24) - macros.timedelta(minutes=var.value.daily_ohlcv_data_download_delay_min | int)  }}'",
     "--start_timestamp '{{ execution_date - macros.timedelta(minutes=var.value.daily_ohlcv_data_download_delay_min	| int) }}'",
     "--exchange_id '{}'",
     "--universe '{}'",
     "--aws_profile 'ck'",
     "--data_type '{}'",
     "--file_format 'parquet'",
     # The command needs to be executed manually first because --incremental
     # assumes appending to existing folder.
     "--incremental",
     "--contract_type '{}'",
     "--s3_path '{}{}/{}'"
]

start_task = DummyOperator(task_id='start_dag', dag=dag)
end_download = DummyOperator(task_id='end_dag', dag=dag)

for provider, exchange, contract, data_type in product(_PROVIDERS, _EXCHANGES, _CONTRACTS, _DATA_TYPES):

    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(download_command)
    curr_bash_command[0] = curr_bash_command[0].format(provider)
    curr_bash_command[3] = curr_bash_command[3].format(exchange)
    curr_bash_command[4] = curr_bash_command[4].format(_UNIVERSES[provider])
    curr_bash_command[6] = curr_bash_command[6].format(data_type)
    curr_bash_command[-2] = curr_bash_command[-2].format(contract)
    curr_bash_command[-1] = curr_bash_command[-1].format(
        s3_daily_staged_data_path,
        # For futures we need to suffix the folder.
        "-futures" if contract == "futures" else "",
        provider
    )

    kwargs = {}
    kwargs["network_configuration"] = {
        "awsvpcConfiguration": {
            "securityGroups": ecs_security_group,
            "subnets": ecs_subnets,
        },
    }

    downloading_task = ECSOperator(
        task_id=f"download_{provider}_{exchange}_{contract}",
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
            ]
        },
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
        execution_timeout=datetime.timedelta(minutes=15),
        **kwargs
    )

    start_task >> downloading_task >> end_download