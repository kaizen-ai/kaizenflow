"""
Import as:

import im_v2.airflow.dags.airflow_utils.trade_exec.utils as imvadauteu
"""

import datetime
import os
from typing import List, Optional


# Check for `ModuleNotFoundError` to ensure code works in the repo and Airflow,
# to keep file in sync with Airflow version easily.
try:
    import airflow
    import airflow_utils.misc as misc
except ModuleNotFoundError:
    import im_v2.airflow as airflow
    import im_v2.airflow.dags.airflow_utils.misc as misc

_BASE_NOTEBOOK_DST_DIR = "s3://cryptokaizen-html/notebooks"


def get_flatten_account_cmd(
    exchange: str, secret_id: int, universe: str, log_dir: str
) -> List[str]:
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
    date_path = misc.create_date_path(datetime.datetime.today())
    s3_path = os.path.join(
        f"s3://{airflow.models.Variable.get(f'{stage}_s3_data_bucket')}",
        "tokyo_experiments_compressed",
        date_path,
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
        f"for file in *.tar.gz; do tar -xzf $file -C {efs_mount_eu}/{stage}/ --no-same-owner; done",
    ]


def build_system_run_cmd(
    strategy: str,
    dag_builder_ctor_as_str: str,
    trade_date: str,
    liveness: str,
    instance_type: str,
    exchange: str,
    stage: str,
    account_type: str,
    secret_id: int,
    verbosity: str,
    run_duration: str,
    run_mode: str,
    start_time: str,
    log_base_dir: str,
    dag_run_mode: str,
    set_config_value_params: Optional[List[str]],
) -> List[str]:
    """
    Build bash command for a system run.

    See param descriptions in `run_Cx_prod_system.py`
    """
    # TODO(gp): Add dasserts to make sure don't pass wrong types or use a library to enforce the types at runtime.
    cmd = [
        "/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py",
        f"--strategy '{strategy}'",
        f"--dag_builder_ctor_as_str '{dag_builder_ctor_as_str}'",
        f"--trade_date {trade_date}",
        f"--liveness '{liveness}'",
        f"--instance_type '{instance_type}'",
        f"--exchange '{exchange}'",
        f"--stage '{stage}'",
        f"--account_type '{account_type}'",
        f"--secret_id '{secret_id}'",
        f"-v '{verbosity}' 2>&1",
        f"--run_duration {run_duration}",
        f"--run_mode '{run_mode}'",
        f"--start_time '{start_time}'",
        f"--log_file_name '{log_base_dir}/logs/log.{dag_run_mode}.txt'",
        f"--dst_dir '{log_base_dir}/system_log_dir.{dag_run_mode}'",
    ]
    if set_config_value_params:
        cmd += set_config_value_params
    return cmd


def build_system_reconcile_cmd(
    root_dir: str,
    dag_builder_ctor_as_str: str,
    start_timestamp: str,
    end_timestamp: str,
    dag_run_mode: str,
    system_run_mode: str,
    system_stage: str,
    tag: str,
    mark_as_last_24_hour_run: bool,
    set_config_value_params: Optional[List[str]],
) -> List[str]:
    """
    Build bash command for a system reconciliation run.

    See param descriptions in `reconcile_run_all`
    """
    # Get a dir to store reconciliation results in.
    reconc_log_base_dir = os.path.join(root_dir, "prod_reconciliation")
    # Get a path to the prod run outcomes.
    source_dir = os.path.join(root_dir, "system_reconciliation")
    # Build a list of command lines.
    cmd = [
        # This is a hack to pass hserver.is_inside_docker()
        #  because when ran in ECS it is not present there
        #  by default.
        "mkdir /.dockerenv",
        "&&",
        "invoke reconcile_run_all",
        f"--dag-builder-ctor-as-str '{dag_builder_ctor_as_str}'",
        f"--dst-root-dir {reconc_log_base_dir}",
        f"--prod-data-source-dir {source_dir}",
        f"--start-timestamp-as-str {start_timestamp}",
        f"--end-timestamp-as-str {end_timestamp}",
        f"--mode {dag_run_mode}",
        f"--run-mode '{system_run_mode}'",
        "--no-prevent-overwriting",
        "--run-notebook",
        f"--stage {system_stage}",
        f"--tag {tag}",
        "--backup-dir-if-exists",
    ]
    if mark_as_last_24_hour_run:
        cmd += ["--mark-as-last-24-hour-run"]
    if set_config_value_params:
        cmd += set_config_value_params
    return cmd
