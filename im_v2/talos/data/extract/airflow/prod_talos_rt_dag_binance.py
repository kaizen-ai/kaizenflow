import datetime

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

_STAGE = "prod"
_EXCHANGE = "binance"
_PROVIDER = "talos"

ecs_cluster = Variable.get(f"{_STAGE}_ecs_cluster")
# The naming convention is set such that this value is then reused
# in log groups, stream prefixes and container names to minimize
# convolution and maximize simplicity.
ecs_task_definition = Variable.get(f"{_STAGE}_ecs_task_definiton")
ecs_subnets = [Variable.get("ecs_subnet1"), Variable.get("ecs_subnet2")]
ecs_security_group = [Variable.get("ecs_security_group")]
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_definition}"

# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
    "email_on_failure": False,
    "owner": "airflow",
}

# Create a command.
bash_command = [
    f"/app/im_v2/{_PROVIDER}/data/extract/download_realtime_for_one_exchange.py",
    "--start_timestamp {{ execution_date - macros.timedelta(minutes=5) }}",
    "--end_timestamp {{ execution_date }}",
    f"--exchange_id '{_EXCHANGE}'",
    "--universe 'v03'",
    "--db_stage 'dev'",
    "--db_table 'ccxt_ohlcv'",
    "--aws_profile 'ck'",
    f"--s3_path 's3://{Variable.get(f'{_STAGE}_s3_data_bucket')}/{Variable.get('s3_realtime_data_folder')}/{_PROVIDER}'",
]

# Create a DAG.
dag = airflow.DAG(
    dag_id=f"{_STAGE}_realtime_{_PROVIDER}_{_EXCHANGE}",
    description=f"Realtime download of {_PROVIDER} OHLCV data from {_EXCHANGE}",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 1, 31, 0, 0, 0),
)

# Run the script with ECS operator.
downloading_task = ECSOperator(
    task_id=f"realtime_{_PROVIDER}_{_EXCHANGE}",
    dag=dag,
    aws_conn_id=None,
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="EC2",
    overrides={
        "containerOverrides": [
            {
                "name": f"{ecs_task_definition}",
                "command": bash_command,
                "environment": [
                    {
                        "name": "ENABLE_DIND",
                        "value": "0",
                    },
                ],
            }
        ]
    },
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix,
)
# Execute the DAG.
downloading_task  # pylint: disable=pointless-statement