# This is a utility DAG to download historic `bid_ask` data backfill.
import copy
import datetime
import os

import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
dag_type = "datapull"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]

# Used for seperations of deployment environments
# ignored when executing on prod/preprod.
_USERNAME = ""

# Deployment type, if the task should be run via fargate (serverless execution)
# or EC2 (machines deployed in our auto-scaling group)
_LAUNCH_TYPE = "fargate"


_DAG_ID = components_dict["dag_id"]
_EXCHANGES = [components_dict["exchange"]]
_VENDORS = [components_dict["vendor"]]
_UNIVERSES = components_dict["universe"]
_CONTRACTS = ["futures"]
_DATA_TYPES = [components_dict["data_type"]]
# The value is implicit since this is an Airflow DAG.
_DAG_DESCRIPTION = f"Daily {_DATA_TYPES} data download, contracts:" \
                + f"{_CONTRACTS}, using {_VENDORS} from {_EXCHANGES}."
# Specify when/how often to execute the DAG.
_SCHEDULE = "@weekly"
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)
# Pass default parameters for the DAG.
default_args = {
    "retries": 0,
    "email": [Variable.get(f'{_STAGE}_notification_email')],
    "email_on_failure": False if _STAGE in ["prod", "preprod"] else False,
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
    start_date=datetime.datetime(2020, 6, 1, 0, 0, 0),
    end_date=datetime.datetime(2023, 5, 27, 0, 0, 0),
    tags=[_STAGE],
)

download_command = [
    "python /app/amp/im_v2/binance/data/extract/download_historical_bid_ask.py",
    "--start_date '{{ (data_interval_start).replace(hour=0, minute=0, second=0)}}'",
    "--stop_date '{{ (data_interval_start).replace(hour=0, minute=0, second=0) + macros.timedelta(days=7)}}'",
    "--incremental",
    "--secret_name 'binance.preprod.trading.10'",
    "--stage '{}'",
    "--universe '{}'",
    "--bid_ask_data_type '{}'",
]

start_task = DummyOperator(task_id='start_dag', dag=dag)
end_download = DummyOperator(task_id='end_dag', dag=dag)

for  data_type in ["T_DEPTH"]:

    #TODO(Juraj): Make this code more readable.
    # Do a deepcopy of the bash command list so we can reformat params on each iteration.
    curr_bash_command = copy.deepcopy(download_command)
    curr_bash_command[-3] = curr_bash_command[-3].format(_STAGE)
    curr_bash_command[-2] = curr_bash_command[-2].format(_UNIVERSES)
    curr_bash_command[-1] = curr_bash_command[-1].format(data_type)

    downloading_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"download.bid_ask.binance.{data_type}",
        curr_bash_command,
        _ECS_TASK_DEFINITION,
        512, 
        2048,
    )

    start_task >> downloading_task >> end_download