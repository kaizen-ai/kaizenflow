import datetime
import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import airflow_utils.telegram.operator as aiutteop
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

_DAG_DESCRIPTION = f"Data archival from EFS to S3 glacier"
_SCHEDULE = "0 23 * * *"
_S3_ARCHIVAL_LOCATION = "s3://cryptokaizen-data-backup/efs_archive"
_EFS_DIRECTORY = "/data/shared/ecs/*/*reconciliation/"
_RETENTION_PERIOD = 14
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)

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
    start_date=datetime.datetime(2024, 2, 5, 0, 0, 0),
)

archive_command = [
    # aws s3 cp/sync commands does not support preservation of dir structure in the source directory.
    # https://serverfault.com/questions/682708/copy-directory-structure-intact-to-aws-s3-bucket
    f"for entry in $(find {_EFS_DIRECTORY} -mindepth 3 -maxdepth 3 -mtime +{_RETENTION_PERIOD} -type d); do",
    f"aws s3 cp --recursive $entry {_S3_ARCHIVAL_LOCATION}$entry/ ;",
    "done",
    "&&",
    f"find {_EFS_DIRECTORY} -mindepth 3 -maxdepth 3 -mtime +{_RETENTION_PERIOD} -type d -exec rm -rf {{}} \;"
]

archiving_task = aiutecop.get_ecs_run_task_operator(
        dag,
        _STAGE,
        f"archive_efs_to_s3",
        archive_command,
        _ECS_TASK_DEFINITION, 
        1024, 
        4096,
)