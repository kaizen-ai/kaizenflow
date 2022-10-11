# #############################################################################
# Reconciliation
# #############################################################################

# Logically the reconciliation process:
# 1) Create reconciliation shared dirs
# 2) Run prod system (done by a separate AirFlow task)
# 3) Copy prod data to a shared folder
# 4) Dump market data for simulation
# 5) Run simulation
# 6) Copy simulation data to a shared folder
# 7) Dump TCA data (not implemented yet)
# 8) Run the reconciliation notebook and publish it

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


def _system(cmd: str) -> int:
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _dassert_is_date(date: str) -> None:
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


def _get_run_date(run_date: Optional[str]) -> str:
    """
    Return the run date.

    If a date is not specified by a user then return current date.
    """
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
    # Create a dir specific of the run date.
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


@task
def reconcile_dump_market_data(ctx, run_date=None, incremental=False, interactive=True):  # type: ignore
    # pylint: disable=line-too-long
    """
    Dump the market data image and save it to a shared folder.

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
        opts = "-s --dbg"
        docker_cmd = (
            f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name} {opts}"
        )
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
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(target_dir)
    # Copy market data file to the target dir.
    cmd = f"cp -v {market_data_file} {target_dir}"
    _system(cmd)
    # Prevent overwriting.
    market_data_file_shared = os.path.join(target_dir, market_data_file)
    cmd = f"chmod -w {market_data_file_shared}"
    _system(cmd)
    # Sanity check remote data.
    _sanity_check_data(market_data_file_shared)


@task
def reconcile_run_sim(ctx, run_date=None):  # type: ignore
    """
    Run the simulation given a run date.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = "system_log_dir"
    if os.path.exists(target_dir):
        rm_cmd = f"rm -rf {target_dir}"
        _LOG.warning("The target_dir=%s already exists, removing it.", target_dir)
        _system(rm_cmd)
    # Run simulation.
    opts = "-s --dbg --update_outcomes"
    # pylint: disable=line-too-long
    test_name = "dataflow_orange/system/C1/test/test_C1b_prod_system.py::Test_C1b_Time_ForecastSystem_with_DataFramePortfolio_ProdReconciliation::test_run_simulation"
    # pylint: enable=line-too-long
    docker_cmd = f"AM_RECONCILE_SIM_DATE={run_date} pytest_log {test_name} {opts}"
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    _system(cmd)
    # Check that system log dir exists and is not empty.
    hdbg.dassert_dir_exists(os.path.join(target_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(target_dir, "process_forecasts"))


@task
def reconcile_copy_sim_data(ctx, run_date=None):  # type: ignore
    """
    Copy the output of the simulation run to a shared folder.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date, "simulation")
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results to '%s'", target_dir)
    # Copy the output to the shared folder.
    system_log_dir = "./system_log_dir"
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    _system(docker_cmd)
    # Copy simulation run logs to the shared folder.
    pytest_log_file_path = "tmp.pytest_script.txt"
    hdbg.dassert_file_exists(pytest_log_file_path)
    docker_cmd = f"cp -v {pytest_log_file_path} {target_dir}"
    _system(docker_cmd)


@task
def reconcile_copy_prod_data(ctx, run_date=None, stage="preprod"):  # type: ignore
    """
    Copy the output of the prod run to a shared folder.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date, "prod")
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results to '%s'", target_dir)
    # Copy prod run results to the target dir.
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    # Prod system is run via AirFlow and the results are tagged with the previous day.
    prod_run_date = (run_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    shared_dir = f"/data/shared/ecs/{stage}"
    cmd = f"find '{shared_dir}' -name system_log_dir_scheduled__*2hours | grep '{prod_run_date}'"
    # E.g., `.../system_log_dir_scheduled__2022-10-03T10:00:00+00:00_2hours`.
    _, system_log_dir = hsystem.system_to_string(cmd)
    hdbg.dassert_dir_exists(system_log_dir)
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    _system(docker_cmd)
    # Copy prod run logs to the shared folder.
    cmd = f"find '{shared_dir}/logs' -name log_scheduled__*2hours.txt | grep '{prod_run_date}'"
    # E.g., `.../log_scheduled__2022-10-05T10:00:00+00:00_2hours.txt`.
    _, log_file = hsystem.system_to_string(cmd)
    hdbg.dassert_file_exists(log_file)
    docker_cmd = f"cp -v {log_file} {target_dir}"
    _system(docker_cmd)
    # Prevent overwriting.
    cmd = f"chmod -R -w {target_dir}"
    _system(cmd)


@task
def reconcile_run_notebook(ctx, run_date=None):  # type: ignore
    """
    Run the reconciliation notebook, publish it locally and copy the results to
    the shared folder.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    # TODO(Grisha): pass `asset_class` as a param.
    asset_class = "crypto"
    #
    cmd_txt = []
    cmd_txt.append(f"export AM_RECONCILIATION_DATE={run_date}")
    cmd_txt.append(f"export AM_ASSET_CLASS={asset_class}")
    # Add the command to run the notebook.
    notebook_path = "amp/oms/notebooks/Master_reconciliation.ipynb"
    config_builder = "amp.oms.reconciliation.build_reconciliation_configs()"
    opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt"
    dst_dir = "."
    # pylint: disable=line-too-long
    cmd_run_txt = f"amp/dev_scripts/notebooks/run_notebook.py --notebook {notebook_path} --config_builder '{config_builder}' --dst_dir {dst_dir} {opts}"
    # pylint: enable=line-too-long
    cmd_txt.append(cmd_run_txt)
    cmd_txt = "\n".join(cmd_txt)
    # Save the commands as a script.
    file_name = "tmp.publish_notebook.sh"
    hio.to_file(file_name, cmd_txt)
    # Run the script inside docker.
    _LOG.info("Running the notebook=%s", notebook_path)
    docker_cmd = f"invoke docker_cmd --cmd 'source {file_name}'"
    _system(docker_cmd)
    # Copy the published notebook to the shared folder.
    results_dir = os.path.join(dst_dir, "result_0")
    hdbg.dassert_dir_exists(results_dir)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, run_date)
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    docker_cmd = f"cp -vr {results_dir} {target_dir}"
    _system(docker_cmd)
    # Prevent overwriting.
    results_shared_dir = os.path.join(target_dir, "result_0")
    cmd = f"chmod -R -w {results_shared_dir}"
    _system(cmd)


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


@task
def reconcile_dump_tca_data(ctx, run_date=None):  # type: ignore
    """
    Retrieve and save the TCA data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    # TODO(Grisha): add as params to the interface.
    end_timestamp = run_date
    start_timestamp = run_date - datetime.timedelta(days=1)
    dst_dir = "./tca"
    exchange_id = "binance"
    contract_type = "futures"
    stage = "preprod"
    account_type = "trading"
    secrets_id = "3"
    universe = "v7.1"
    # pylint: disable=line-too-long
    opts = f"--exchange_id {exchange_id} --contract_type {contract_type} --stage {stage} --account_type {account_type} --secrets_id {secrets_id} --universe {universe}"
    log_file = os.path.join(dst_dir, "log.txt")
    cmd_run_txt = f"amp/oms/get_ccxt_fills.py --start_timestamp {start_timestamp} --end_timestamp {end_timestamp} --dst_dir {dst_dir} {opts} --incremental -v DEBUG 2>&1 | tee {log_file}"
    # pylint: enable=line-too-long
    # Save the command as a script.
    file_name = "tmp.dump_tca_data.sh"
    hio.to_file(file_name, cmd_run_txt)
    # Run the script inside docker.
    docker_cmd = f"invoke docker_cmd --cmd 'source {file_name}'"
    _system(docker_cmd)
    # Copy dumped data to a shared folder.
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, "tca")
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", dst_dir, target_dir)
    docker_cmd = f"cp -vr {dst_dir} {target_dir}"
    _system(docker_cmd)
    # Prevent overwriting.
    f"chmod -R -w {target_dir}"
    _system(docker_cmd)


@task
def reconcile_run_all(ctx, run_date=None):  # type: ignore
    """
    Run all phases of prod vs simulation reconciliation.
    """
    reconcile_create_dirs(ctx, run_date=run_date)
    #
    reconcile_copy_prod_data(ctx, run_date=run_date)
    #
    reconcile_dump_market_data(ctx, run_date=run_date)
    reconcile_run_sim(ctx, run_date=run_date)
    reconcile_copy_sim_data(ctx, run_date=run_date)
    #
    # TODO(gp): Download for the day before.
    reconcile_run_notebook(ctx, run_date=run_date)
    reconcile_ls(ctx, run_date=run_date)
    reconcile_dump_tca_data(ctx, run_date=None)
