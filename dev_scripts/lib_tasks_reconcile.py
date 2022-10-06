# #############################################################################
# Reconciliation
# #############################################################################

# Logically the reconciliation process:
# 1) Run prod system
# 2) Dump market data
# 3) Run sim system
# 4) Dump TCA data
# 5) Run notebook and publish it
#
# User specifies start and end of prod run and everything gets derived from those
# dates.

"""
Import as:

import helpers.lib_tasks_docker as hlitadoc
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Match, Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hio as hio
import helpers.hsystem as hsystem


_LOG = logging.getLogger(__name__)

_PROD_RECONCILIATION_DIR = "/data/shared/prod_reconciliation"

def _system(cmd):
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


# TODO(gp): It seems that the data is written by the prod system in the right
#  location already. Maybe nothing to do.
# Copy data from /data/shared/ecs/test/system_log_dir_scheduled__2022-10-02T10:00:00+00:00_2hours
# cp -a '/data/shared/ecs/test/system_log_dir_scheduled__2022-10-02T10:00:00+00:00_2hours'/* /data/shared/prod_reconciliation/20221003/prod/
# @task
# def reconcile_dump_prod_data(ctx, account_type, incremental=False):  # type: ignore
#     """
#     Dump prod data for the last run in the given account_type.
#     """
#     hdbg.dassert_in(account_type, ("live_trading", "candidate", "qa"))
#     file_name = f"prod_submissions.{account_type}.log"
#     if incremental and os.path.exists(file_name):
#         _LOG.warning("Reusing existing %s", file_name)
#     else:
#         # TODO(gp): Dump data.
#         pass
#     #
#     hdbg.dassert_is_not(job_id, None)
#     hdbg.dassert_is_integer(job_id)
#     _LOG.info(hprint.to_str("job_id"))
#     #
#     today = datetime.date.today()
#     if today != run_date:
#         _LOG.warning("Running this script not on the trade date" +
#             hprint.to_str("today run_date"))
#     # > TARGET_DIR={_PROD_RECONCILIATION_DIR}/$RUN_DATE/
#     target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}/job.{account_type}.{job_id}"
#     _LOG.info(hprint.to_str("target_dir"))
#     hio.create_dir(target_dir, incremental=True)
#     # > RUN_DATE=20220914
#     # > JOBID=1002436514
#     # > TARGET_DIR={_PROD_RECONCILIATION_DIR}/$RUN_DATE/job.$JOBID; echo $TARGET_DIR
#     if account_type in ("live_trading", "candidate"):
#         ...
#     elif account_type == "qa":
#         ...
#     else:
#         raise ValueError(f"Invalid account_type='{account_type}'")
#     cmd += f" 2>&1 | tee {file_name}"
#     _system(cmd)
#     #
#     cmd = f"ls -lh {target_dir}"
#     _system(cmd)
#     # TODO(gp): It doesn't always work. Maybe we should save it locally and then
#     #  cp as user.
#     #_system(cmd)


def _dassert_is_date(date: str) -> None:
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


# import pwd
# 
# 
# def get_username() -> str:
#     return pwd.getpwuid(os.getuid())[0]
# 
# 
# def is_owned_by_user(file_name: str) -> bool:
#     hdbg.dassert_path_exists(file_name)
#     user_owner = pwd.getpwuid(os.stat(file_name).st_uid).pw_name
#     return get_username() == user_owner


def _get_run_date(run_date: Optional[str]) ->  str:
    if run_date is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    _LOG.info(hprint.to_str("run_date"))
    _dassert_is_date(run_date)
    return run_date


def _sanity_check_data(file_path: str) -> None:
    """
    Check that data at the specified file path is correct.
    """
    cmd = f"gzip -cd {file_path} | head -3"
    _system(cmd)
    cmd = f"gzip -cd {file_path} | tail -3"
    _system(cmd)
    cmd = f"gzip -cd {file_path} | wc -l"
    _system(cmd)
    cmd = f"ls -lh {file_path}"
    _system(cmd)


@task
def reconcile_create_dirs(ctx, run_date=None):  # type: ignore
    """
    Create dirs for storing reconciliation data.

    Final dirs layout is:
    ```
    data/
        shared/
            prod_reconciliation/
                {run_date}/
                    prod/
                    ...
                    simulation/
    ```
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    # Create run date dir.
    run_date_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date)
    hio.create_dir(run_date_dir, incremental=True)
    # Create dirs for storing prod and simulation results.
    prod_dir = os.path.join(run_date_dir, "prod")
    simulation_dir = os.path.join(run_date_dir, "simulation")
    hio.create_dir(prod_dir, incremental=True)
    hio.create_dir(simulation_dir, incremental=True)
    # Sanity check the created dirs.
    cmd = f"ls -lh {run_date_dir}"
    _system(cmd)


# > pytest_log dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_save_data -s --dbg
# > cp -v test_data.csv.gz /data/shared/prod_reconciliation/20220928/simulation
@task
def reconcile_dump_market_data(ctx, run_date=None, incremental=False, interactive=True):  # type: ignore
    """
    Dump the market data image and save it in the proper dir.

    The output df looks like:
    ```
                                 asset_id              knowledge_timestamp     open     high      low    close     volume            end_download_timestamp       id        full_symbol            start_timestamp
    end_timestamp
    2022-09-27 10:35:00-04:00  1030828978 2022-09-27 14:36:12.230330+00:00   0.7233   0.7247   0.7215   0.7241  3291381.0  2022-09-27 14:36:11.727196+00:00  3409049  binance::GMT_USDT  2022-09-27 10:34:00-04:00
    2022-09-27 10:35:00-04:00  1464553467 2022-09-27 14:36:05.788923+00:00  1384.23  1386.24  1383.19  1385.63   5282.272  2022-09-27 14:36:05.284506+00:00  3409043  binance::ETH_USDT  2022-09-27 10:34:00-04:00
    ```

    :param run_date: date of the reconcile run
    :param incremental: if False then the directory is deleted and re-created,
        otherwise it skips
    :param interactive: if True ask user to visually inspect data snippet and
        confirm the dumping
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    market_data_file = "test_data.csv.gz"
    if incremental and os.path.exists(market_data_file):
        _LOG.warning("Skipping generating %s", market_data_file)
    else:
        test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_save_data"
        docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name}"
        # TODO(Grisha): enable debug mode.
        #docker_cmd += " -s --dbg"
        cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
        _system(cmd)
    hdbg.dassert_file_exists(market_data_file)
    # Check the market data file.
    _sanity_check_data(market_data_file)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    #
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date, "simulation")
    _LOG.info(hprint.to_str("target_dir"))
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    hdbg.dassert_dir_exists(target_dir)
    cmd = f"ls {target_dir}"
    _system(cmd)
    # Copy market data file to the target dir.
    cmd = f"cp -v {market_data_file} {target_dir}"
    _system(cmd)
    # Disable file re-writing.
    market_data_file_shared = os.path.join(target_dir, market_data_file)
    cmd = f"chmod -R -w {market_data_file_shared}"
    _system(cmd)
    # Sanity check remote data.
    _sanity_check_data(market_data_file_shared)


# TODO(gp): Move it to a more general library.
# @task
# def backup_file(ctx, file_name, action="move", dst_dir=None, timestamp=None, mark_as_read_only=True,
#                 abort_on_missing=True):  # type: ignore
#     """
#     Backup a file using a timestamp.

#     :param action: `move` or `copy`
#     :param dst_dir: destination dir for the backup. If `None` save the file in the
#         same dir as `file_name`
#     :param timestamp: use the given timestamp or get a new one if `None`
#     :param abort_on_missing: abort if the source file doesn't exist
#     """
#     _ = ctx
#     hdbg.dassert_in(action, ("move", "copy"))
#     # Create target file.
#     if timestamp is None:
#         timestamp = hlitauti.get_ET_timestamp()
#     #
#     suffix = f".backup.{timestamp}"
#     dst_file_name = hio.add_suffix_to_filename(file_name, suffix, with_underscore=False)
#     if dst_dir is not None:
#         # The destination file is in `dst_dir`.
#         hdbg.dassert_isinstance(dst_dir, str)
#         base_name = os.path.basename(file_name)
#         hio.create_dir(dst_dir, incremental=True)
#         dst_file_name = os.path.join(dst_dir, base_name)
#     _LOG.info(hprint.to_str("dst_file_name"))
#     hdbg.dassert_path_not_exists(dst_file_name)
#     # Check if exists.
#     if abort_on_missing:
#         hdbg.dassert_path_exists(file_name)
#     # Rename the file.
#     if os.path.exists(file_name):
#         if action == "move":
#             cmd = "mv"
#         elif action == "copy":
#             cmd = "cp -a"
#         else:
#             raise ValueError(f"Invalid action='{action}'")
#         cmd = f"{cmd} {file_name} {dst_file_name}"
#         _system(cmd)
#         _LOG.info("Backed up '%s' as '%s'", file_name, dst_file_name)
#         if mark_as_read_only:
#             cmd = f"chmod -w -R {dst_file_name}"
#             _system(cmd)
#     else:
#         _LOG.warning("File '%s' doesn't exist: skipping", file_name)
#     return dst_file_name
# 
# 
# def delete_file(file_name):
#     # TODO(gp): Do not allow to delete dir with Git or current dir.
#     #hdbg.dassert_ne(file_name, ".")
#     if os.path.exists(file_name):
#         cmd = f"chmod +w {file_name}; rm -rf {file_name}"
#         if not is_owned_by_user(file_name):
#             cmd = f"sudo bash -c '{cmd}'"
#         hsystem.system(cmd)


# > rm -r system_log_dir/; pytest_log ./dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test1 -s --dbg --update_outcomes
@task
def reconcile_run_sim(ctx, run_date=None):  # type: ignore
    """
    Run reconciliation simulation for `run_date`.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    #target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    #_LOG.info(hprint.to_str("target_dir"))
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    # Remove local dir with system logs.
    # TODO(Grisha): @Dan Pass target dir as a param.
    target_dir = "system_log_dir"
    if os.path.exists(target_dir):
        rm_cmd = f"rm -rf {target_dir}"
        _system(rm_cmd)
    # Run simulation.
    opts = "-s --dbg --update_outcomes"
    test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_run_simulation"
    docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name} {opts}"
    #docker_cmd += "; exit ${PIPESTATUS[0]})"
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    _system(cmd)
    # Check that system log dir exists and is filled.
    hdbg.dassert_dir_exists(os.path.join(target_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(target_dir, "process_forecasts"))


# > cp -vr ./system_log_dir /data/shared/prod_reconciliation/{run_date}/simulation/
# > cp -v tmp.pytest_script.txt /data/shared/prod_reconciliation/{run_date}/simulation/
@task
def reconcile_copy_sim_data(ctx, run_date=None):  # type: ignore
    """
    Copy the output of the simulation in the proper dir.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date, "simulation")
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Saving results to '%s'", target_dir)
    # Save system logs.
    system_log_dir = "./system_log_dir"
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    _system(docker_cmd)
    # Save script logs.
    pytest_log_file_path = "tmp.pytest_script.txt"
    hdbg.dassert_file_exists(pytest_log_file_path)
    docker_cmd = f"cp -v {pytest_log_file_path} {target_dir}"
    _system(docker_cmd)


# > cp -vr system_log_dir_{run_date}_2hours /data/shared/prod_reconciliation/{run_date}/prod/
# > cp -v log_{run_date}_2hours.txt /data/shared/prod_reconciliation/{run_date}/prod/
@task
def reconcile_copy_prod_data(ctx, run_date=None):  # type: ignore
    """
    Copy the output of the prod in the proper dir.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date, "prod")
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Saving results to '%s'", target_dir)
    # Copy system log file to the target dir.
    # TODO(Grisha): pass stage as a param with `preprod` as a default value.
    # Actual prod data dir contains a date of the previous day in its name.
    previous_day_date_str = (
        datetime.datetime.strptime(run_date, "%Y%m%d") - datetime.timedelta(days=1)
    ).strftime('%Y-%m-%d')
    system_log_dir = f"/data/shared/ecs/preprod/system_log_dir_scheduled__{previous_day_date_str}T10:00:00+00:00_2hours"
    hdbg.dassert_dir_exists(system_log_dir)
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    # TODO(Grisha): @Dan change permission to disable overwriting.
    _system(docker_cmd)
    # TODO(Grisha): @Dan pick up the logs from `/data/shared/ecs/preprod/logs`.
    # # Copy script log file to the target dir.
    # log_file = f"log_{run_date}_2hours.txt"
    # hdbg.dassert_file_exists(system_log_dir)
    # docker_cmd = f"cp -v {log_file} {target_dir}"
    # _system(docker_cmd)


@task
def reconcile_run_notebook(ctx, run_date=None):
    """
    Run reconcilation notebook.
    """
    _ = ctx
    run_date = "20221005" #_get_run_date(run_date)
    asset_class = "crypto"
    target_dir = "./"
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Saving results to '%s'", target_dir)
    #
    notebook_file = "amp/oms/notebooks/Master_reconciliation.ipynb"
    # TODO(Grisha): @Dan Fix issue with parenthesis.
    config_builder = "amp.oms.reconciliation.get_reconciliation_config()"
    cmd_ = [f'AM_RECONCILIATION_DATE={run_date} AM_ASSET_CLASS={asset_class} amp/dev_scripts/notebooks/run_notebook.py']
    cmd_.append(f'--notebook {notebook_file}')
    cmd_.append(f'--config_builder "{config_builder}"')
    cmd_.append(f'--dst_dir {target_dir}')
    cmd_.append("--num_threads 'serial'")
    # TODO(Grisha): @Dan Understand whether the notebook is published and where.
    cmd_.append("--publish_notebook")
    docker_cmd = " ".join(cmd_)
    #
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    # TODO(Grisha): @Dan Add command to copy the published notebook from local to shared.
    _system(cmd)


# TODO(Danya): Add script here to dump fills data.
# amp/oms/get_ccxt_fills.py \
#    --start_timestamp '2022-10-02' \
#    --end_timestamp '2022-10-03' \
#    --dst_dir '/shared_data/prod_reconciliation/20221003/tca' \
#    --exchange_id 'binance' \
#    --contract_type 'futures' \
#    --stage 'preprod' \
#    --account_type 'trading' \
#    --secrets_id '3' \
#    --universe 'v7.1' \
#    --incremental
# TCA = transaction_cost_analysis
# @task
# def reconcile_dump_tca_data(ctx, run_date=None):  # type: ignore
#     """
#     Retrieve and save the TCA data.
#     """
#     _ = ctx
#     run_date = _get_run_date(run_date)
#     target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     hio.create_dir(target_dir, incremental=True)
#     # TODO(gp): run fills downloading script.
#     cmd = f"aws s3 cp --recursive {src_dir} {dst_dir}"
#     _system(cmd)
#     #
#     cmd = f"ls -lh {target_dir}"
#     _system(cmd)
#     cmd = f"du -d 1 -h {target_dir}"
#     _system(cmd)


# TODO(gp): Run reconciliation notebook and publish.
# @task
# def reconcile_run_notebook()
# ...


@task
def reconcile_run_all(ctx, run_date=None):  # type: ignore
    """
    Run all phases for reconciling a prod run.

    - dump market data
    - dump prod live trading and candidate
    - run simulation
    - run notebook
    """
    run_date = _get_run_date(run_date)
    reconcile_create_dirs(ctx, run_date=run_date)
    #
    reconcile_copy_prod_data(ctx, run_date=run_date)
    #
    reconcile_dump_market_data(ctx, run_date=run_date)
    reconcile_run_sim(ctx, run_date=run_date)
    reconcile_copy_sim_data(ctx, run_date=run_date)
    #
    # TODO(gp): Download for the day before.
    #reconcile_dump_tca_data(ctx, run_date=None)
    reconcile_ls(ctx, run_date=run_date)


# #############################################################################


@task
def reconcile_ls(ctx, run_date=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date)
    _LOG.info(hprint.to_str("target_dir"))
    hdbg.dassert_dir_exists(target_dir)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


# @task
# def reconcile_rmrf(ctx, run_date=None):  # type: ignore
#     run_date = _get_run_date(run_date)
#     reconcile_ls(ctx, run_date)
#     target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}"
#     _LOG.info(hprint.to_str("target_dir"))
#     cmd = f"chmod +w -R {target_dir}; rm -rf {target_dir}"
#     print(f"> {cmd}")
