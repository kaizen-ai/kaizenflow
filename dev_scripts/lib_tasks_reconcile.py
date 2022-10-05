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

import dev_scripts.lib_tasks_reconcile as dslitare
"""

import datetime
import logging
import os
from typing import Optional

from invoke import task

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)

_PROD_RECONCILIATION_DIR = "/data/shared/prod_reconciliation"


def _system(cmd):
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _dassert_is_date(date: str) -> None:
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


def _get_run_date(run_date: Optional[str]) -> str:
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
    Create dirs for storing reconciliation data. Final dirs layout is: ```
    data/ shared/ prod_reconciliation/

    {run_date}/                 prod/                 ...
    simulation/ ```
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

# pylint: disable=line-too-long
# > pytest_log dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_save_data -s --dbg
# > cp -v test_data.csv.gz /data/shared/prod_reconciliation/20220928/simulation
# pylint: enable=line-too-long
@task
def reconcile_dump_market_data(ctx, run_date=None, incremental=False, interactive=True):  # type: ignore
    # pylint: disable=line-too-long
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
    # pylint: enable=line-too-long
    _ = ctx
    run_date = _get_run_date(run_date)
    market_data_file = "test_data.csv.gz"
    if incremental and os.path.exists(market_data_file):
        _LOG.warning("Skipping generating %s", market_data_file)
    else:
        # pylint: disable=line-too-long
        test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_save_data"
        # pylint: enable=line-too-long
        docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name}"
        # TODO(Grisha): enable debug mode.
        # docker_cmd += " -s --dbg"
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


# pylint: disable=line-too-long
# > rm -r system_log_dir/; pytest_log ./dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test1 -s --dbg --update_outcomes
# pylint: enable=line-too-long
@task
def reconcile_run_sim(ctx, run_date=None):  # type: ignore
    """
    Run reconciliation simulation for `run_date`.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    # target_dir = f"{_PROD_RECONCILIATION_DIR}/{run_date}/simulation"
    # _LOG.info(hprint.to_str("target_dir"))
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
    # pylint: disable=line-too-long
    test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_run_simulation"
    # pylint: enable=line-too-long
    docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name} {opts}"
    # docker_cmd += "; exit ${PIPESTATUS[0]})"
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
    # TODO(Grisha): @Dan Pass run duration as a param.
    # TODO(Grisha): @Dan infer the path from run date.
    # TODO(Grisha): pass stage as a param with `preprod` as a default value.
    system_log_dir = f"/data/shared/ecs/preprod/system_log_dir_scheduled__2022-10-03T10:00:00+00:00_2hours"
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
    # reconcile_dump_tca_data(ctx, run_date=None)
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
