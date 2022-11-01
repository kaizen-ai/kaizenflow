# #############################################################################
# Data Reconciliation
# #############################################################################
"""
Invokes in the file are runnable from a Docker container only.

E.g., to run for certain date from a Docker container:
```
> invoke run_reconcile_run_all --run-date 20221017
```

to run outside a Docker container:
```
> invoke docker_cmd --cmd 'invoke run_reconcile_run_all --run-date 20221017'
```

Import as:

import dev_scripts.lib_tasks_data_reconcile as dslitadr
"""

import datetime
import logging
import os
import sys
from typing import Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hserver as hserver
import helpers.hsystem as hsystem
import core.config as cconfig

import dev_scripts.lib_tasks_reconcile as dslitare

_LOG = logging.getLogger(__name__)


def _reconcile_data_create_dirs(
    base_dst_dir, start_timestamp, end_timestamp, db_table, abort_if_exists=True
) -> str:
    """
    Create dirs for storing data reconciliation results.

    Final dirs layout is:
    ```
    {base_dst_dir}/
        {timestamp_dst_dir}/
            {db_table}/
                result_0/
            ...
    ```
    
    i.e.
    ```
    shared_data/ecs/preprod/data_reconciliation/
         2022-10-21_00:00:00_2022-10-21_21:00:00 \
             ccxt_ohlcv_futures_preprod/
                result_0
            ...
    ```

    :param base_dst_dir: base directory (most likely shared) to store data
     reconciliation
    :param start_timestamp: start of the reconciled time interval
    :param end_timestamp: end of the reconciled time interval
    :param db_table: DB table the reconciliation is done for, at the moment assuming
     there is only a single reconcilation done on a particular table and particular 
     time range.
    :param abort_if_exists: see `hio.create_dir()`
    :return: path to the created target dir
    """
    # Strip the timezone part of the timestamps to improve readability
    #  the context isn't lost since the raw args are present in the saved notebook
    #  and also chances of using anything else as UTC are low.
    timestamp_dst_dir = start_timestamp.rstrip("+00:00") + "_" + end_timestamp.rstrip("+00:00") 
    target_dir = os.path.join(base_dst_dir, timestamp_dst_dir, db_table)
    # Create a dir for reconcilation results.
    hio.create_dir(target_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Sanity check the created dirs.
    cmd = f"ls -lh {target_dir}"
    dslitare._system(cmd)
    return target_dir


def build_dummy_config() -> cconfig.ConfigList:
    """
    Dummy function to pass into amp/dev_scripts/notebooks/run_notebook.py
    as a configu_builder parameter
    """
    config = cconfig.Config.from_dict({"dummy": "value"})
    config_list = cconfig.ConfigList([config])
    return config_list


# TODO(Juraj): this flow is very similiar to dslitare.reconcile_run_notebook
#  it might be good to define common behavior.
@task
def run_data_reconciliation_notebook(
   ctx,
   stage,
   db_stage,
   start_timestamp,
   end_timestamp,
   exchange_id,
   data_type,
   contract_type,
   db_table,
   aws_profile,
   s3_vendor,
   s3_path,
   base_dst_dir,
   bid_ask_accuracy=None,
   resample_mode=None
):  # type: ignore
    """
    Run data reconciliation notebook and store in in a stored location.
    
    See `im_v2.ccxt.data.extract.compare_realtime_and_historical` for
    reconcilation params description.

    :param stage: stage at which the reconciliation is executed, 
     influence placement of the results.
    :param base_dst_dir: dir to store data reconciliation
    """
    env_var_name_base = "DATA_RECONCILE_"
    os.environ[env_var_name_base + "DB_STAGE"] = db_stage
    os.environ[env_var_name_base + "START_TIMESTAMP"] = start_timestamp
    os.environ[env_var_name_base + "END_TIMESTAMP"] = end_timestamp
    os.environ[env_var_name_base + "EXCHANGE_ID"] = exchange_id
    os.environ[env_var_name_base + "DATA_TYPE"] = data_type
    os.environ[env_var_name_base + "CONTRACT_TYPE"] = contract_type
    os.environ[env_var_name_base + "DB_TABLE"] = db_table
    os.environ[env_var_name_base + "AWS_PROFILE"] = aws_profile
    os.environ[env_var_name_base + "S3_VENDOR"] = s3_vendor
    os.environ[env_var_name_base + "S3_PATH"] = s3_path
    os.environ[env_var_name_base + "BID_ASK_ACCURACY"] = str(bid_ask_accuracy)
    os.environ[env_var_name_base + "RESAMPLE_MODE"] = str(resample_mode)
    _ = ctx
    # Dir to store notebook locally.
    # results_dir = os.path.join(".", "result_0")
    # Add the command to run the notebook.
    notebook_path = "amp/im_v2/ccxt/notebooks/Data_reconciliation.ipynb"
    cmd_txt = []
    # TODO(Juraj): rewrite env variables logic via core.config.config_builder 
    #  if desired for code consistency.
    config_builder = "amp.dev_scripts.lib_tasks_data_reconcile.build_dummy_config()"
    opts = "--num_threads 'serial' --allow_errors --publish_notebook -v DEBUG 2>&1"
    cmd_run_txt = [
        "amp/dev_scripts/notebooks/run_notebook.py",
        f"--notebook {notebook_path}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir '.'",
        f"{opts}",
    ]
    cmd_run_txt = " ".join(cmd_run_txt)
    cmd_txt.append(cmd_run_txt)
    cmd_txt = "\n".join(cmd_txt)
    # Save the commands as a script.
    script_name = "tmp.publish_notebook.sh"
    hio.create_executable_script(script_name, cmd_txt)
    # Make the script executable and run it.
    _LOG.info("Running the notebook=%s", notebook_path)
    dslitare._system(script_name)
    # Copy the published notebook to the specified folder.
    #target_dir = _reconcile_data_create_dirs(base_dst_dir, start_timestamp, end_timestamp, db_table)
    #hdbg.dassert_dir_exists(target_dir)
    #_LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    #cmd = f"cp -vr {results_dir} {target_dir}"
    #dslitare._system(cmd)
