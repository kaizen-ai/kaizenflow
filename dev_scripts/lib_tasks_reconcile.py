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
# 7) Dump TCA data
# 8) Run the reconciliation notebook and publish it

"""
Import as:

import dev_scripts.lib_tasks_reconcile as dslitare
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


def _get_folder_name(
    run_date: Optional[str], folder_name: Optional[str]
) -> str:
    """
    Return the folder name to store reconcilation results.

    If a folder name is not specified by a user then use run date.
    """
    if folder_name is None:
        run_date = _get_run_date(run_date)
        folder_name = run_date
    _LOG.info(hprint.to_str("folder_name"))
    return folder_name


# TODO(Dan): Expose `rt_timeout_in_secs_or_time` as datetime as well.
def _get_rt_timeout_in_secs_or_time(
    rt_timeout_in_secs_or_time: Optional[int]
) -> int:
    """
    Return the number of seconds to run reconcilation for.

    If a the param is not specified by a user then use default 2 hours.
    """
    if rt_timeout_in_secs_or_time is None:
        rt_timeout_in_secs_or_time = 2 * 60 * 60
    _LOG.info(hprint.to_str("rt_timeout_in_secs_or_time"))
    return rt_timeout_in_secs_or_time


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
def reconcile_create_dirs(
    ctx, run_date=None, folder_name=None, abort_if_exists=True
):  # type: ignore
    """
    Create dirs for storing reconciliation data.

    Final dirs layout is:
    ```
    data/
        shared/
            prod_reconciliation/
                {folder_name}/
                    prod/
                    tca/
                    simulation/
                    ...
    ```
    """
    _ = ctx
    folder_name = _get_folder_name(run_date, folder_name)
    # Create a dir specific of the run.
    results_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name)
    hio.create_dir(
        results_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dirs for storing prod and simulation results.
    prod_dir = os.path.join(results_dir, "prod")
    simulation_dir = os.path.join(results_dir, "simulation")
    hio.create_dir(prod_dir, incremental=True, abort_if_exists=abort_if_exists)
    hio.create_dir(
        simulation_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dir for dumped TCA data.
    tca_dir = os.path.join(results_dir, "tca")
    hio.create_dir(tca_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Sanity check the created dirs.
    cmd = f"ls -lh {results_dir}"
    _system(cmd)


@task
def reconcile_dump_market_data(
    ctx, run_date=None, folder_name=None, incremental=False, interactive=False
):  # type: ignore
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
    :param folder_name: name of the directory to store reconcilation results at
    :param incremental: if False then the directory is deleted and re-created,
        otherwise it skips
    :param interactive: if True ask user to visually inspect data snippet and
        confirm the dumping
    """
    # pylint: enable=line-too-long
    _ = ctx
    run_date = _get_run_date(run_date)
    folder_name = _get_folder_name(run_date, folder_name)
    market_data_file = "test_data.csv.gz"
    # TODO(Grisha): @Dan Reconsider clause logic (compare with `reconcile_run_notebook`).
    if incremental and os.path.exists(market_data_file):
        _LOG.warning("Skipping generating %s", market_data_file)
    else:
        # TODO(Grisha): @Dan Copy logs to the shared folder.
        # pylint: disable=line-too-long
        opts = f"--action dump_data --reconcile_sim_date {run_date} -v DEBUG 2>&1 | tee reconcile_dump_market_data_log.txt"
        # pylint: enable=line-too-long
        script_name = "dataflow_orange/system/C1/C1b_reconcile.py"
        docker_cmd = f"{script_name} {opts}"
        cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
        _system(cmd)
    hdbg.dassert_file_exists(market_data_file)
    # Check the market data file.
    _sanity_check_data(market_data_file)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    #
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name, "simulation")
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
def reconcile_run_sim(
    ctx, run_date=None, folder_name=None, rt_timeout_in_secs_or_time=None
):  # type: ignore
    """
    Run the simulation given a run date.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    folder_name = _get_folder_name(run_date, folder_name)
    rt_timeout_in_secs_or_time = _get_rt_timeout_in_secs_or_time(
        rt_timeout_in_secs_or_time
    )
    target_dir = "system_log_dir"
    if os.path.exists(target_dir):
        rm_cmd = f"rm -rf {target_dir}"
        _LOG.warning("The target_dir=%s already exists, removing it.", target_dir)
        _system(rm_cmd)
    # Run simulation.
    # pylint: disable=line-too-long
    opts = f"--action run_simulation --reconcile_sim_date {run_date} --folder_name {folder_name} --rt_timeout_in_secs_or_time {rt_timeout_in_secs_or_time} -v DEBUG 2>&1 | tee reconcile_run_sim_log.txt"
    # pylint: enable=line-too-long
    script_name = "dataflow_orange/system/C1/C1b_reconcile.py"
    docker_cmd = f"{script_name} {opts}"
    cmd = f"invoke docker_cmd --cmd '{docker_cmd}'"
    _system(cmd)
    # Check that system log dir exists and is not empty.
    hdbg.dassert_dir_exists(os.path.join(target_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(target_dir, "process_forecasts"))
    # TODO(Grisha): @Dan Add asserts on the latest files so we confirm that simulation was completed.


@task
def reconcile_copy_sim_data(ctx, run_date=None, folder_name=None):  # type: ignore
    """
    Copy the output of the simulation run to a shared folder.
    """
    _ = ctx
    folder_name = _get_folder_name(run_date, folder_name)
    target_dir = os.path.join(
        _PROD_RECONCILIATION_DIR, folder_name, "simulation"
    )
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results to '%s'", target_dir)
    # Copy the output to the shared folder.
    system_log_dir = "./system_log_dir"
    docker_cmd = f"cp -vr {system_log_dir} {target_dir}"
    _system(docker_cmd)
    # Copy simulation run logs to the shared folder.
    pytest_log_file_path = "reconcile_run_sim_log.txt"
    hdbg.dassert_file_exists(pytest_log_file_path)
    docker_cmd = f"cp -v {pytest_log_file_path} {target_dir}"
    _system(docker_cmd)


@task
def reconcile_copy_prod_data(
    ctx, run_date=None, folder_name=None, stage="preprod"
):  # type: ignore
    """
    Copy the output of the prod run to a shared folder.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    folder_name = _get_folder_name(run_date, folder_name)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name, "prod")
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
def reconcile_run_notebook(
    ctx,
    run_date=None,
    folder_name=None,
    rt_timeout_in_secs_or_time=None,
    incremental=False,
):  # type: ignore
    """
    Run the reconciliation notebook, publish it locally and copy the results to
    the shared folder.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    folder_name = _get_folder_name(run_date, folder_name)
    # Set results destination dir and clear it if is already filled.
    dst_dir = "."
    results_dir = os.path.join(dst_dir, "result_0")
    if os.path.exists(results_dir):
        if incremental:
            _LOG.warning(
                "Notebook run results are already stored at %s", results_dir
            )
            sys.exit(-1)
        else:
            rm_cmd = f"rm -rf {results_dir}"
            _LOG.warning(
                "The results_dir=%s already exists, removing it.", results_dir
            )
            _system(rm_cmd)
    # TODO(Grisha): pass `asset_class` as a param.
    asset_class = "crypto"
    #
    cmd_txt = []
    cmd_txt.append(f"export AM_RECONCILIATION_DATE={run_date}")
    cmd_txt.append(f"export RT_TIMEOUT_IN_SECS_OR_TIME={rt_timeout_in_secs_or_time}")
    cmd_txt.append(f"export AM_ASSET_CLASS={asset_class}")
    # Add the command to run the notebook.
    notebook_path = "amp/oms/notebooks/Master_reconciliation.ipynb"
    config_builder = "amp.oms.reconciliation.build_reconciliation_configs()"
    opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt"
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
    hdbg.dassert_dir_exists(results_dir)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name)
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    docker_cmd = f"cp -vr {results_dir} {target_dir}"
    _system(docker_cmd)
    # Prevent overwriting.
    results_shared_dir = os.path.join(target_dir, "result_0")
    cmd = f"chmod -R -w {results_shared_dir}"
    _system(cmd)


@task
def reconcile_ls(ctx, run_date=None, folder_name=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.
    """
    _ = ctx
    folder_name = _get_folder_name(run_date, folder_name)
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name)
    _LOG.info(hprint.to_str("target_dir"))
    hdbg.dassert_dir_exists(target_dir)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


@task
def reconcile_dump_tca_data(
    ctx, run_date=None, folder_name=None, incremental=False
):  # type: ignore
    """
    Retrieve and save the TCA data.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    folder_name = _get_folder_name(run_date, folder_name)
    # TODO(Grisha): add as params to the interface.
    end_timestamp = run_date
    start_timestamp = (
        end_timestamp - datetime.timedelta(days=1)
    ).strftime("%Y%m%d")
    dst_dir = "./tca"
    if os.path.exists(dst_dir):
        if incremental:
            _LOG.warning("TCA data is already stored at %s", dst_dir)
            sys.exit(-1)
        else:
            _LOG.warning(
                "The dst_dir=%s already exists, re-creating it.", dst_dir
            )
    hio.create_dir(dst_dir, incremental=incremental)
    exchange_id = "binance"
    contract_type = "futures"
    stage = "preprod"
    account_type = "trading"
    secrets_id = "3"
    universe = "v7.1"
    # pylint: disable=line-too-long
    opts = f"--exchange_id {exchange_id} --contract_type {contract_type} --stage {stage} --account_type {account_type} --secrets_id {secrets_id} --universe {universe}"
    log_file = os.path.join(dst_dir, "log.txt")
    cmd_run_txt = f"amp/oms/get_ccxt_fills.py --start_timestamp '{start_timestamp}' --end_timestamp '{end_timestamp}' --dst_dir {dst_dir} {opts} --incremental -v DEBUG 2>&1 | tee {log_file}"
    # pylint: enable=line-too-long
    # Save the command as a script.
    file_name = "tmp.dump_tca_data.sh"
    hio.to_file(file_name, cmd_run_txt)
    # Run the script inside docker.
    docker_cmd = f"invoke docker_cmd --cmd 'source {file_name}'"
    _system(docker_cmd)
    # Copy dumped data to a shared folder.
    target_dir = os.path.join(_PROD_RECONCILIATION_DIR, folder_name)
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", dst_dir, target_dir)
    docker_cmd = f"cp -vr {dst_dir} {target_dir}"
    _system(docker_cmd)
    # Prevent overwriting.
    f"chmod -R -w {target_dir}"
    _system(docker_cmd)


@task
def reconcile_run_all(ctx, run_date=None, folder_name=None, rt_timeout_in_secs_or_time=None):  # type: ignore
    """
    Run all phases of prod vs simulation reconciliation.
    """
    reconcile_create_dirs(ctx, run_date=run_date, folder_name=folder_name)
    #
    reconcile_copy_prod_data(ctx, run_date=run_date, folder_name=folder_name)
    #
    reconcile_dump_market_data(ctx, run_date=run_date, folder_name=folder_name)
    reconcile_run_sim(
        ctx,
        run_date=run_date,
        folder_name=folder_name,
        rt_timeout_in_secs_or_time=rt_timeout_in_secs_or_time,
    )
    reconcile_copy_sim_data(ctx, run_date=run_date, folder_name=folder_name)
    #
    reconcile_dump_tca_data(ctx, run_date=run_date, folder_name=folder_name)
    #
    reconcile_run_notebook(
        ctx,
        run_date=run_date,
        folder_name=folder_name,
        rt_timeout_in_secs_or_time=rt_timeout_in_secs_or_time,
    )
    reconcile_ls(ctx, run_date=run_date, folder_name=folder_name)
