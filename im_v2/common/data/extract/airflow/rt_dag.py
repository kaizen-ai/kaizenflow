import datetime
from itertools import product

import airflow
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

_STAGE = "test"
_EXCHANGES = ["binance", "coinbase"]
_PROVIDERS = ["ccxt", "talos"]

# E.g. DB table ccxt_ohlcv -> has an equivalent for testing ccxt_ohlcv_test
# but production is ccxt_ohlcv.
_TABLE_SUFFIX = f"_{_STAGE}" if _STAGE == "test" else ""

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
    "email": [Variable.get(f"{_STAGE}_notification_email")],
    "email_on_failure": True,
    "email_on_retry": True,
    "owner": "airflow",
}

# Create a command, leave values to be parametrized.
bash_command = [
    "/app/im_v2/{}/data/extract/download_realtime_for_one_exchange.py",
    "--start_timestamp {{ execution_date - macros.timedelta(minutes=5) }}",
    "--end_timestamp {{ execution_date }}",
    "--exchange_id '{}'",
    "--universe 'v03'",
    "--db_stage 'dev'",
    "--db_table '{}_ohlcv{}'",
    "--aws_profile 'ck'",
    "--s3_path 's3://{}/{}/{}'",
]

# Create a DAG.
dag = airflow.DAG(
    dag_id=f"{_STAGE}_realtime_dag",
    description=f"Realtime download of {_PROVIDERS} OHLCV data from {_EXCHANGES}",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    start_date=datetime.datetime(2022, 3, 1, 0, 0, 0),
)

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

for provider, exchange in product(_PROVIDERS, _EXCHANGES):

    # TODO(Juraj): make this code more readable
    bash_command[0] = bash_command[0].format(provider)
    bash_command[3] = bash_command[3].format(exchange)
    bash_command[-3] = bash_command[-3].format(provider, _TABLE_SUFFIX)
    bash_command[-1] = bash_command[-1].format(
        Variable.get(f"{_STAGE}_s3_data_bucket"),
        Variable.get("s3_realtime_data_folder"),
        provider,
    )

    downloading_task = ECSOperator(
        task_id=f"rt_{provider}_v2_{exchange}",
        dag=dag,
        aws_conn_id=None,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="EC2",
        overrides={
            "containerOverrides": [
                {
                    "name": "cmamp-test",
                    "command": bash_command,
                    "environment": [
                        {
                            "name": "DATA_INTERVAL_START",
                            "value": "{{ execution_date - macros.timedelta(minutes=3) }}",
                        },
                        {
                            "name": "DATA_INTERVAL_END",
                            "value": "{{ execution_date }}",
                        },
                    ],
                }
            ]
        },
        awslogs_group=ecs_awslogs_group,
        awslogs_stream_prefix=ecs_awslogs_stream_prefix,
    )
    # Define the sequence of execution of task.
    start_task >> downloading_task >> end_task
