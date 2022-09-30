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
from typing import Any, Dict, List, Match, Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hio as hio
import helpers.hsystem as hsystem


_LOG = logging.getLogger(__name__)

PROD_RECONCILIATION_DIR = "/data/shared/prod_reconciliation"

def _system(cmd):
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


# TODO(gp): It seems that the data is written by the prod system in the right
#  location already. Maybe nothing to do.
@task
def reconcile_dump_prod_data(ctx, account_type, incremental=False):  # type: ignore
    """
    Dump prod data for the last run in the given account_type.
    """
    hdbg.dassert_in(account_type, ("live_trading", "candidate", "qa"))
    file_name = f"prod_submissions.{account_type}.log"
    if incremental and os.path.exists(file_name):
        _LOG.warning("Reusing existing %s", file_name)
    else:
        # TODO(gp): Dump data.
        pass
    #
    hdbg.dassert_is_not(job_id, None)
    hdbg.dassert_is_integer(job_id)
    _LOG.info(hprint.to_str("job_id"))
    #
    today = datetime.date.today()
    if today != run_date:
        _LOG.warning("Running this script not on the trade date" +
            hprint.to_str("today run_date"))
    # > TARGET_DIR={PROD_RECONCILIATION_DIR}/$RUN_DATE/
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}/job.{account_type}.{job_id}"
    _LOG.info(hprint.to_str("target_dir"))
    hio.create_dir(target_dir, incremental=True)
    # > RUN_DATE=20220914
    # > JOBID=1002436514
    # > TARGET_DIR={PROD_RECONCILIATION_DIR}/$RUN_DATE/job.$JOBID; echo $TARGET_DIR
    if account_type in ("live_trading", "candidate"):
        ...
    elif account_type == "qa":
        ...
    else:
        raise ValueError(f"Invalid account_type='{account_type}'")
    cmd += f" 2>&1 | tee {file_name}"
    _system(cmd)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    # TODO(gp): It doesn't always work. Maybe we should save it locally and then
    #  cp as user.
    #_system(cmd)


def _dassert_is_date(date: str) -> None:
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


import pwd


def get_username() -> str:
    return pwd.getpwuid(os.getuid())[0]


def is_owned_by_user(file_name: str) -> bool:
    hdbg.dassert_path_exists(file_name)
    user_owner = pwd.getpwuid(os.stat(file_name).st_uid).pw_name
    return get_username() == user_owner


def _get_run_date(run_date: Optional[str]) ->  str:
    if run_date is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    _LOG.info(hprint.to_str("run_date"))
    _dassert_is_date(run_date)
    return run_date


@task
def reconcile_create_dirs(ctx, run_date=None):  # type: ignore
    """
    Create dirs for storing recinciliation data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    run_date_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
    hio.create_dir(run_date_dir, incremental=True)
    #
    prod_dir = f"{run_date_dir}/prod"
    simulation_dir = f"{run_date_dir}/simulation"
    hio.create_dir(prod_dir, incremental=True)
    hio.create_dir(simulation_dir, incremental=True)


# > pytest_log dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_save_data -s --dbg
# > cp -v test_data.csv.gz /data/shared/prod_reconciliation/20220928/simulation
@task
def reconcile_dump_market_data(ctx, run_date=None, incremental=False, interactive=True):  # type: ignore
    """
    Dump the market data image and save it in the proper dir.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_file = "test_data.csv.gz"
    if incremental and os.path.exists(target_file):
        _LOG.warning("Skipping generating %s", target_file)
    else:
        docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::save_data"
        #docker_cmd += " -s --dbg"
        cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
        _system(cmd)
    hdbg.dassert_file_exists(target_file)
    #
    # Check the target_file.
    cmd = f"gzip -cd {target_file} | head -3"
    _system(cmd)
    cmd = f"gzip -cd {target_file} | tail -3"
    _system(cmd)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    #
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    _LOG.info(hprint.to_str("target_dir"))
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    hdbg.dassert_dir_exists(target_dir)
    #
    cmd = f"ls {target_dir}"
    _system(cmd)
    #
    cmd = f"cp -v {target_file} {target_dir}"
    _system(cmd)
    # cmd = f"chmod -R -w {target_file}"
    # _system(cmd)
    # Sanity check remote data.
    cmd = f"gzip -cd {target_file} | head -3"
    _system(cmd)
    cmd = f"gzip -cd {target_file} | tail -3"
    _system(cmd)
    cmd = f"gzip -cd {target_file} | wc -l"
    _system(cmd)
    cmd = f"ls -lh {target_file}"
    _system(cmd)


# TODO(gp): Move it to a more general library.
@task
def backup_file(ctx, file_name, action="move", dst_dir=None, timestamp=None, mark_as_read_only=True,
                abort_on_missing=True):  # type: ignore
    """
    Backup a file using a timestamp.

    :param action: `move` or `copy`
    :param dst_dir: destination dir for the backup. If `None` save the file in the
        same dir as `file_name`
    :param timestamp: use the given timestamp or get a new one if `None`
    :param abort_on_missing: abort if the source file doesn't exist
    """
    _ = ctx
    hdbg.dassert_in(action, ("move", "copy"))
    # Create target file.
    if timestamp is None:
        timestamp = hlitauti.get_ET_timestamp()
    #
    suffix = f".backup.{timestamp}"
    dst_file_name = hio.add_suffix_to_filename(file_name, suffix, with_underscore=False)
    if dst_dir is not None:
        # The destination file is in `dst_dir`.
        hdbg.dassert_isinstance(dst_dir, str)
        base_name = os.path.basename(file_name)
        hio.create_dir(dst_dir, incremental=True)
        dst_file_name = os.path.join(dst_dir, base_name)
    _LOG.info(hprint.to_str("dst_file_name"))
    hdbg.dassert_path_not_exists(dst_file_name)
    # Check if exists.
    if abort_on_missing:
        hdbg.dassert_path_exists(file_name)
    # Rename the file.
    if os.path.exists(file_name):
        if action == "move":
            cmd = "mv"
        elif action == "copy":
            cmd = "cp -a"
        else:
            raise ValueError(f"Invalid action='{action}'")
        cmd = f"{cmd} {file_name} {dst_file_name}"
        _system(cmd)
        _LOG.info("Backed up '%s' as '%s'", file_name, dst_file_name)
        if mark_as_read_only:
            cmd = f"chmod -w -R {dst_file_name}"
            _system(cmd)
    else:
        _LOG.warning("File '%s' doesn't exist: skipping", file_name)
    return dst_file_name


def delete_file(file_name):
    # TODO(gp): Do not allow to delete dir with Git or current dir.
    #hdbg.dassert_ne(file_name, ".")
    if os.path.exists(file_name):
        cmd = f"chmod +w {file_name}; rm -rf {file_name}"
        if not is_owned_by_user(file_name):
            cmd = f"sudo bash -c '{cmd}'"
        hsystem.system(cmd)


# > rm -r system_log_dir/; pytest_log ./dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test1 -s --dbg --update_outcomes
@task
def reconcile_run_sim(ctx, run_date=None):  # type: ignore
    """
    Run reconciliation simulation for `run_date`.
    """
    timestamp = hlitauti.get_ET_timestamp()
    #
    run_date = _get_run_date(run_date)
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    _LOG.info(hprint.to_str("target_dir"))
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    hdbg.dassert_dir_exists(target_dir)
    # Run simulation.
    opts = "-s --dbg --update_outcomes"
    test_name = "./dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::run_simulation"
    docker_cmd = f"pytest {test_name} {opts} 2>&1 | tee {log_file}"
    docker_cmd = f"rm -r system_log_dir/; {docker_cmd}"
    #docker_cmd += "; exit ${PIPESTATUS[0]})"
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    _system(cmd)


# > cp -vr ./system_log_dir /data/shared/prod_reconciliation/20220928/simulation/
# > cp -v tmp.pytest_script.txt /data/shared/prod_reconciliation/20220928/simulation/
@task
def reconcile_save_sim(ctx, run_date=None, timestamp=None):  # type: ignore
    """
    Copy the output of the simulation in the proper dir.
    """
    run_date = _get_run_date(run_date)
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    _LOG.info("Saving results to '%s'", target_dir)
    # If the target dir doesn't exist we didn't downloaded the test data and we can't
    # continue.
    hdbg.dassert_dir_exists(target_dir)
    #
    system_log_dir = "./system_log_dir"
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    _system(cmd)
    #
    pytest_script_file_path = "tmp.pytest_script.txt"
    docker_cmd = f"cp -v {pytest_script_file_path} {target_dir}"
    _system(cmd)


# TODO(Danya): Add script here to dump fills data.
# TCA = transaction_cost_analysis
@task
def reconcile_dump_tca_data(ctx, run_date=None):  # type: ignore
    """
    Retrieve and save the TCA data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
    _LOG.info(hprint.to_str("target_dir"))
    hio.create_dir(target_dir, incremental=True)
    # TODO(gp): run fills downloading script.
    cmd = f"aws s3 cp --recursive {src_dir} {dst_dir}"
    _system(cmd)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


# TODO(gp): Run reconciliation notebook and publish.
# @task
# def reconcile_run_notebook()
# ...


@task
def reconcile_run_all(ctx, incremental=False):  # type: ignore
    """
    Run all phases for reconciling a prod run.

    - dump market data
    - dump prod live trading and candidate
    - run simulation
    - run notebook
    """
    run_date = "20220928"
    reconcile_create_dirs(ctx, run_date=run_date)
    reconcile_dump_market_data(ctx, run_date=run_date)
    #
    # TODO(Dan): Add prod invokes.
    #
    reconcile_run_sim(ctx, account_type, run_date=run_date)
    reconcile_save_sim(ctx, account_type, run_date=run_date)
    #
    # TODO(gp): Download for the day before.
    #reconcile_dump_tca_data(ctx, run_date=None)
    reconcile_ls(ctx, run_date=None)


# #############################################################################


@task
def reconcile_ls(ctx, run_date=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
    _LOG.info(hprint.to_str("target_dir"))
    hdbg.dassert_dir_exists(target_dir)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


@task
def reconcile_rmrf(ctx, run_date=None):  # type: ignore
    run_date = _get_run_date(run_date)
    reconcile_ls(ctx, run_date)
    target_dir = f"{PROD_RECONCILIATION_DIR}/{run_date}"
    _LOG.info(hprint.to_str("target_dir"))
    cmd = f"chmod +w -R {target_dir}; rm -rf {target_dir}"
    print(f"> {cmd}")
