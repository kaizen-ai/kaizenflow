# This is a utility DAG to conduct QA on real time data download
# The task compares the downloaded data with the contents of
# of the database, to confirm a match or show discrepancies.

# IMPORTANT NOTES:
# Make sure to set correct dag schedule `schedule_interval`` parameter.

# This DAG's configuration deploys tasks to AWS Fargate to offload the EC2s
# mainly utilized for rt download
import copy
import datetime
import os

import airflow
import airflow_utils.telegram.operator as aiutteop
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
# _STAGE = _FILENAME.split(".")[0]
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
# List of dataset signatures to perform QA on. i.e.:
# "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
_QA_JOBS = [
    "realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7.ccxt.binance.v1_0_0",
]
# Shared location to store the reconciliaiton notebook into
_QA_NB_DST_DIR = os.path.join(
    "{{ var.value.efs_mount }}", _STAGE, "data_qa", "periodic_10min"
)
_DAG_DESCRIPTION = (
    "Data QA. Run QA notebook and publish results" + " to a shared EFS."
)
_SCHEDULE = Variable.get(f"{_DAG_ID}_schedule")

# Used for container overrides inside DAG task definition.
# If this is a test DAG don't forget to add your username to container suffix.
# i.e. cmamp-test-juraj since we try to follow the convention of container having
# the same name as task-definition if applicable
# Set to the name your task definition is suffixed with i.e. cmamp-test-juraj,
_CONTAINER_SUFFIX = f"-{_STAGE}" if _STAGE in ["preprod", "test"] else ""
_CONTAINER_SUFFIX += f"-{_USERNAME}" if _STAGE == "test" else ""
_CONTAINER_NAME = f"cmamp{_CONTAINER_SUFFIX}"

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
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
s3_bucket = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f"{_STAGE}_notification_email")],
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
    start_date=datetime.datetime(2023, 1, 24, 0, 0, 0),
)

invoke_cmd = [
    "invoke run_single_dataset_qa_notebook",
    f"--stage '{_STAGE}'",
    "--start-timestamp '{{ data_interval_start.replace(second=0) - macros.timedelta(minutes=5) }}'",
    "--end-timestamp '{{ data_interval_end.replace(second=0) - macros.timedelta(minutes=5) }}'",
    "--dataset-signature '{}'",
    "--aws-profile 'ck'",
    f"--base-dst-dir '{_QA_NB_DST_DIR}'",
]

start_comparison = DummyOperator(task_id="start_comparison", dag=dag)
end_comparison = DummyOperator(task_id="end_comparison", dag=dag)

for dataset_signature in _QA_JOBS:
    # Do a deepcopy of the bash cmd list so we can reformat params on each iteration.
    curr_invoke_cmd = copy.deepcopy(invoke_cmd)
    curr_invoke_cmd[4] = curr_invoke_cmd[4].format(dataset_signature)

    # We first execute the notebook which finishes successfully regardless of the success of
    #  the reconciliation (unless the notebook execution itself fails, in which case we get
    #  notified). Afterwards a script is executed, return code of the command will inform
    #  Airflow which can send a notification upon failure.
    kwargs = {}
    kwargs["network_configuration"] = {
        "awsvpcConfiguration": {
            "securityGroups": ecs_security_group,
            "subnets": ecs_subnets,
        },
    }

    comparing_task = ECSOperator(
        task_id=f"data_qa.{dataset_signature}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type=_LAUNCH_TYPE.upper(),
        overrides={
            "containerOverrides": [
                {
                    "name": _CONTAINER_NAME,
                    "command": curr_invoke_cmd,
                }
            ],
            "cpu": "512",
            "memory": "2048",
        },
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
        execution_timeout=datetime.timedelta(minutes=15),
        **kwargs,
    )

    start_comparison >> comparing_task >> end_comparison

if _STAGE != "test":
    telegram_notification_task = aiutteop.get_telegram_operator(
        dag, _STAGE, _DAG_ID, "{{ run_id }}"
    )
    end_comparison >> telegram_notification_task
