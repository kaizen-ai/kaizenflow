from typing import List
import os
import airflow
import airflow_utils.misc as aiutmisc
import datetime

_BASE_NOTEBOOK_DST_DIR = "s3://cryptokaizen-html/notebooks"

def get_flatten_account_cmd(exchange: str, secret_id: int, universe: str, log_dir: str) -> List[str]:
    """
    Get command to flatten account.

    This command is used before/after trading experiment.
    """
    return [
        "amp/oms/broker/ccxt/scripts/flatten_ccxt_account.py",
        f"--log_dir {log_dir}",
        f"--exchange '{exchange}'",
        "--contract_type 'futures'",
        f"--stage 'prod'",
        f"--secret_id {secret_id}",
        "--assert_on_non_zero",
        f"--universe '{universe}'",
    ]

def get_log_full_bid_ask_data_cmd(universe: str, vendor: str, exchange_id: str) -> List[str]:
    """
    Get partially formed command to log bid ask data.

    Used for full system experiment to store all of
    the used bid/ask data.
    """
    return [
        "amp/im_v2/ccxt/db/log_experiment_data.py",
        #TODO(Juraj): #Cmtask9052 remove this hardcoded value.
        "--db_stage 'preprod'",
        "--start_timestamp_as_str '{}'",
        "--end_timestamp_as_str '{}'",
        "--log_dir '{}/system_log_dir.{}/process_forecasts'",
        f"--data_vendor '{vendor}'",
        f"--universe '{universe}'",
        f"--exchange_id '{exchange_id}'"
    ]


def get_run_notebook_cmd(log_dir: str) -> List[str]:
    """
    Get command to run notebook after an experiment.
    """
    return [
        "invoke run_notebooks",
        f"--system-log-dir '{log_dir}'",
        f"--base-dst-dir '{_BASE_NOTEBOOK_DST_DIR}'",
    ]

def get_compress_experiment_dir_cmd(stage: str) -> List[str]:
    """
    Get command to compress experiment directory and upload to S3.
    """
    date_path = aiutmisc.create_date_path(datetime.datetime.today())
    s3_path = os.path.join(
        f"s3://{airflow.models.Variable.get(f'{stage}_s3_data_bucket')}",
        "tokyo_experiments_compressed",
        date_path
    )
    efs_mount_tokyo = "{{ var.value.efs_mount_tokyo }}"
    return [
        f"cd {efs_mount_tokyo}/{stage}/",
        "&&",
        "tar -czf /app/{}.tar.gz {}",
        "&&",
        "tar -czf /app/{}.tar.gz {}",
        "&&",
        f"aws s3 sync /app/ {s3_path} --exclude '*' --include '*.tar.gz'",
    ]


def get_move_experiment_to_eu_efs_cmd(stage: str) -> List[str]:
    """
    Get command to fetch experiment from S3, extract into Europe EFS.
    """
    efs_mount_eu = "{{ var.value.efs_mount }}"
    return [
        "aws s3 sync {} . --exclude '*' --include '{}'",
        "&&",
        f"for file in *.tar.gz; do tar -xzf $file -C {efs_mount_eu}/{stage}/ --no-same-owner; done"
    ]
