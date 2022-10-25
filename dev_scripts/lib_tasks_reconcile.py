# #############################################################################
# Reconciliation
# #############################################################################

# Logically the reconciliation process:
# 1) Create reconciliation target dirs
# 2) Run prod system (done by a separate AirFlow task)
# 3) Copy prod data to a target folder
# 4) Dump market data for simulation
# 5) Run simulation
# 6) Copy simulation data to a target folder
# 7) Dump TCA data
# 8) Run the reconciliation notebook and publish it

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
import helpers.hserver as hserver
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


_PROD_RECONCILIATION_DIR = "/shared_data/prod_reconciliation"


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


def _resolve_target_dir(
    dst_dir: Optional[str], run_date: str
) -> str:
    """
    Return the target dir name to store reconcilation results.

    If a dir name is not specified by a user then use prod reconcilation dir
    on the shared disk with the corresponding run date subdir.
    """
    dst_dir = dst_dir or _PROD_RECONCILIATION_DIR
    target_dir = os.path.join(dst_dir, run_date)
    _LOG.info(hprint.to_str("target_dir"))
    return target_dir


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
    ctx, run_date=None, dst_dir=None, abort_if_exists=True
):  # type: ignore
    """
    Create dirs for storing reconciliation data.

    Final dirs layout is:
    ```
    {dst_dir}/
        {run_date}/
            prod/
            tca/
            simulation/
            result_0/
            ...
    ```

    See `reconcile_run_all()` for params description.

    :param abort_if_exists: see `hio.create_dir()`
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    # Create a dir for reconcilation results.
    hio.create_dir(
        target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dirs for storing prod and simulation results.
    prod_target_dir = os.path.join(target_dir, "prod")
    sim_target_dir = os.path.join(target_dir, "simulation")
    hio.create_dir(prod_target_dir, incremental=True, abort_if_exists=abort_if_exists)
    hio.create_dir(
        sim_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dir for dumped TCA data.
    tca_target_dir = os.path.join(target_dir, "tca")
    hio.create_dir(tca_target_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Sanity check the created dirs.
    cmd = f"ls -lh {target_dir}"
    _system(cmd)


@task
def reconcile_dump_market_data(
    ctx,
    run_date=None,
    dst_dir=None,
    incremental=False, 
    interactive=False,
    prevent_overwriting=True,
):  # type: ignore
    # pylint: disable=line-too-long
    """
    Dump the market data image and save it to the specified folder.

    The output df looks like:
    ```
                                 asset_id              knowledge_timestamp     open     high      low    close     volume            end_download_timestamp       id        full_symbol            start_timestamp
    end_timestamp
    2022-09-27 10:35:00-04:00  1030828978 2022-09-27 14:36:12.230330+00:00   0.7233   0.7247   0.7215   0.7241  3291381.0  2022-09-27 14:36:11.727196+00:00  3409049  binance::GMT_USDT  2022-09-27 10:34:00-04:00
    2022-09-27 10:35:00-04:00  1464553467 2022-09-27 14:36:05.788923+00:00  1384.23  1386.24  1383.19  1385.63   5282.272  2022-09-27 14:36:05.284506+00:00  3409043  binance::ETH_USDT  2022-09-27 10:34:00-04:00
    ```

    See `reconcile_run_all()` for params description.

    :param incremental: see `hio.create_dir()`
    :param interactive: if True ask user to visually inspect data snippet and
        confirm the dumping otherwise just save
    """
    # pylint: enable=line-too-long
    hdbg.dassert(hserver.is_inside_docker(), "This is runnable only inside Docker.")
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    market_data_file = "test_data.csv.gz"
    # TODO(Grisha): @Dan Reconsider clause logic (compare with `reconcile_run_notebook`).
    if incremental and os.path.exists(market_data_file):
        _LOG.warning("Skipping generating %s", market_data_file)
    else:
        # TODO(Grisha): @Dan Copy logs to the specified folder.
        # pylint: disable=line-too-long
        opts = f"--action dump_data --reconcile_sim_date {run_date}  -v DEBUG 2>&1 | tee reconcile_dump_market_data_log.txt"
        opts += "; exit ${PIPESTATUS[0]}"
        # pylint: enable=line-too-long
        script_name = "dataflow_orange/system/C1/C1b_reconcile.py"
        cmd = f"{script_name} {opts}"
        _system(cmd)
    hdbg.dassert_file_exists(market_data_file)
    # Check the market data file.
    _sanity_check_data(market_data_file)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    #
    sim_target_dir = os.path.join(target_dir, "simulation")
    _LOG.info(hprint.to_str("sim_target_dir"))
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(sim_target_dir)
    # Copy market data file to the target dir.
    cmd = f"cp -v {market_data_file} {sim_target_dir}"
    _system(cmd)
    # Sanity check result data.
    market_data_file_target = os.path.join(sim_target_dir, market_data_file)
    _sanity_check_data(market_data_file_target)
    #
    if prevent_overwriting:
        _LOG.info("Removing the write permissions for file=%s", market_data_file_target)
        cmd = f"chmod -w {market_data_file_target}"
        _system(cmd)


@task
def reconcile_run_sim(
    ctx, run_date=None, dst_dir=None, rt_timeout_in_secs_or_time=None
):  # type: ignore
    """
    Run the simulation given a run date.

    See `reconcile_run_all()` for params description.
    """
    hdbg.dassert(hserver.is_inside_docker(), "This is runnable only inside Docker.")
    _ = ctx
    run_date = _get_run_date(run_date)
    dst_dir = dst_dir or _PROD_RECONCILIATION_DIR
    # TODO(Dan): Expose `rt_timeout_in_secs_or_time` as datetime as well.
    rt_timeout_in_secs_or_time = rt_timeout_in_secs_or_time or 2 * 60 * 60
    local_results_dir = "system_log_dir"
    if os.path.exists(local_results_dir):
        rm_cmd = f"rm -rf {local_results_dir}"
        _LOG.warning("The local_results_dir=%s already exists, removing it.", local_results_dir)
        _system(rm_cmd)
    # Run simulation.
    # pylint: disable=line-too-long
    opts = f"--action run_simulation --reconcile_sim_date {run_date} --rt_timeout_in_secs_or_time {rt_timeout_in_secs_or_time} --dst_dir {dst_dir} -v DEBUG 2>&1 | tee reconcile_run_sim_log.txt"
    opts += "; exit ${PIPESTATUS[0]}"
    # pylint: enable=line-too-long
    script_name = "dataflow_orange/system/C1/C1b_reconcile.py"
    cmd = f"{script_name} {opts}"
    _system(cmd)
    # Check that system log dir exists and is not empty.
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "process_forecasts"))
    # TODO(Grisha): @Dan Add asserts on the latest files so we confirm that simulation was completed.


@task
def reconcile_copy_sim_data(ctx, run_date=None, dst_dir=None, prevent_overwriting=True):  # type: ignore
    """
    Copy the output of the simulation run to the specified folder.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    sim_target_dir = os.path.join(target_dir, "simulation")
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(sim_target_dir)
    _LOG.info("Copying results to '%s'", sim_target_dir)
    # Copy the output to the specified folder.
    system_log_dir = "./system_log_dir"
    cmd = f"cp -vr {system_log_dir} {sim_target_dir}"
    _system(cmd)
    # Copy simulation run logs to the specified folder.
    pytest_log_file_path = "reconcile_run_sim_log.txt"
    hdbg.dassert_file_exists(pytest_log_file_path)
    cmd = f"cp -v {pytest_log_file_path} {sim_target_dir}"
    _system(cmd)
    if prevent_overwriting:
        _LOG.info("Removing the write permissions for dir=%s", sim_target_dir)
        cmd = f"chmod -R -w {sim_target_dir}"
        _system(cmd)


@task
def reconcile_copy_prod_data(
    ctx,
    run_date=None,
    dst_dir=None,
    stage="preprod",
    prevent_overwriting=True,
):  # type: ignore
    """
    Copy the output of the prod run to the specified folder.

    See `reconcile_run_all()` for params description.

    :param stage: development stage, e.g., `preprod`
    """
    hdbg.dassert_in(stage, ("local", "test", "preprod", "prod"))
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    prod_target_dir = os.path.join(target_dir, "prod")
    # Make sure that the target dir exists before copying.
    hdbg.dassert_dir_exists(prod_target_dir)
    _LOG.info("Copying results to '%s'", prod_target_dir)
    # Copy prod run results to the target dir.
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    # Prod system is run via AirFlow and the results are tagged with the previous day.
    prod_run_date = (run_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    shared_dir = f"/shared_data/ecs/{stage}"
    cmd = f"find '{shared_dir}' -name system_log_dir_scheduled__*2hours | grep '{prod_run_date}'"
    # E.g., `.../system_log_dir_scheduled__2022-10-03T10:00:00+00:00_2hours`.
    _, system_log_dir = hsystem.system_to_string(cmd)
    hdbg.dassert_dir_exists(system_log_dir)
    cmd = f"cp -vr {system_log_dir} {prod_target_dir}"
    _system(cmd)
    # Copy prod run logs to the specified folder.
    cmd = f"find '{shared_dir}/logs' -name log_scheduled__*2hours.txt | grep '{prod_run_date}'"
    # E.g., `.../log_scheduled__2022-10-05T10:00:00+00:00_2hours.txt`.
    _, log_file = hsystem.system_to_string(cmd)
    hdbg.dassert_file_exists(log_file)
    cmd = f"cp -v {log_file} {prod_target_dir}"
    _system(cmd)
    #
    if prevent_overwriting:
        _LOG.info("Removing the write permissions for dir=%s", prod_target_dir)
        cmd = f"chmod -R -w {prod_target_dir}"
        _system(cmd)


# TODO(Grisha): @Dan Expose `rt_timeout_in_secs_or_time` in this invoke.
@task
def reconcile_run_notebook(
    ctx,
    run_date=None,
    dst_dir=None,
    incremental=False,
    prevent_overwriting=True,
):  # type: ignore
    """
    Run the reconciliation notebook, publish it locally and copy the results to
    the specified folder.

    See `reconcile_run_all()` for params description.

    :param incremetal: see `hio.create_dir()`
    """
    hdbg.dassert(hserver.is_inside_docker(), "This is runnable only inside Docker.")
    _ = ctx
    run_date = _get_run_date(run_date)
    # Set results destination dir and clear it if is already filled.
    local_results_dir = "."
    results_dir = os.path.join(local_results_dir, "result_0")
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
    cmd_txt.append(f"export AM_ASSET_CLASS={asset_class}")
    # Add the command to run the notebook.
    notebook_path = "amp/oms/notebooks/Master_reconciliation.ipynb"
    config_builder = "amp.oms.reconciliation.build_reconciliation_configs()"
    opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt; exit ${PIPESTATUS[0]}"
    # pylint: disable=line-too-long
    cmd_run_txt = f"amp/dev_scripts/notebooks/run_notebook.py --notebook {notebook_path} --config_builder '{config_builder}' --dst_dir {dst_dir} {opts}"
    # pylint: enable=line-too-long
    cmd_txt.append(cmd_run_txt)
    cmd_txt = "\n".join(cmd_txt)
    # Save the commands as a script.
    script_name = "tmp.publish_notebook.sh"
    hio.create_executable_script(script_name, cmd_txt)
    # Make the script executable and run it.
    _LOG.info("Running the notebook=%s", notebook_path)
    _system(script_name)
    # Copy the published notebook to the specified folder.
    hdbg.dassert_dir_exists(results_dir)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    cmd = f"cp -vr {results_dir} {target_dir}"
    _system(cmd)
    #
    if prevent_overwriting:
        results_target_dir = os.path.join(target_dir, "result_0")
        _LOG.info("Removing the write permissions for dir=%s", results_target_dir)
        cmd = f"chmod -R -w {results_target_dir}"
        _system(cmd)


@task
def reconcile_ls(ctx, run_date=None, dst_dir=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    _LOG.info(hprint.to_str("target_dir"))
    hdbg.dassert_dir_exists(target_dir)
    #
    cmd = f"ls -lh {target_dir}"
    _system(cmd)
    cmd = f"du -d 1 -h {target_dir}"
    _system(cmd)


@task
def reconcile_dump_tca_data(
    ctx,
    run_date=None,
    dst_dir=None,
    incremental=False,
    prevent_overwriting=True,
):  # type: ignore
    """
    Retrieve and save the TCA data.

    See `reconcile_run_all()` for params description.

    :param incremetal: see `hio.create_dir()`
    """
    hdbg.dassert(hserver.is_inside_docker(), "This is runnable only inside Docker.")
    _ = ctx
    run_date = _get_run_date(run_date)
    target_dir = _resolve_target_dir(dst_dir, run_date)
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    # TODO(Grisha): add as params to the interface.
    end_timestamp = run_date
    start_timestamp = (end_timestamp - datetime.timedelta(days=1)).strftime("%Y%m%d")
    local_results_dir = "./tca"
    if os.path.exists(local_results_dir):
        if incremental:
            _LOG.warning("TCA data is already stored at %s", local_results_dir)
            sys.exit(-1)
        else:
            _LOG.warning(
                "The local_results_dir=%s already exists, re-creating it.",
                local_results_dir,
            )
    hio.create_dir(local_results_dir, incremental=incremental)
    exchange_id = "binance"
    contract_type = "futures"
    stage = "preprod"
    account_type = "trading"
    secrets_id = "3"
    universe = "v7.1"
    # pylint: disable=line-too-long
    opts = f"--exchange_id {exchange_id} --contract_type {contract_type} --stage {stage} --account_type {account_type} --secrets_id {secrets_id} --universe {universe}"
    log_file = os.path.join(local_results_dir, "log.txt")
    cmd_run_txt = f"amp/oms/get_ccxt_fills.py --start_timestamp '{start_timestamp}' --end_timestamp '{end_timestamp}' --dst_dir {local_results_dir} {opts} --incremental -v DEBUG 2>&1 | tee {log_file}"
    # pylint: enable=line-too-long
    cmd_run_txt += "; exit ${PIPESTATUS[0]}"
    # Save the command as a script.
    script_name = "tmp.dump_tca_data.sh"
    hio.create_executable_script(script_name, cmd_run_txt)
    # Make the script executable and run it.
    _system(script_name)
    # Copy dumped data to the specified folder.
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", local_results_dir, target_dir)
    cmd = f"cp -vr {local_results_dir} {target_dir}"
    _system(cmd)
    #
    if prevent_overwriting:
        _LOG.info("Removing the write permissions for dir=%s", target_dir)
        cmd = f"chmod -R -w {target_dir}"
        _system(cmd)


@task
def reconcile_run_all(
    ctx,
    run_date=None,
    dst_dir=None,
    rt_timeout_in_secs_or_time=None,
    prevent_overwriting=True,
    skip_notebook=False,
):  # type: ignore
    """
    Run all phases of prod vs simulation reconciliation.

    :param run_date: date of the reconcile run
    :param dst_dir: dir to store reconcilation results in
    :param rt_timeout_in_secs_or_time: duration of reconcilation run in seconds
    :param prevent_overwriting: if True write permissions are remove otherwise
        a permissions remain as they are
    :param skip_notebook: if True do not run the reconcilation notebook otherwise run
    """
    hdbg.dassert(hserver.is_inside_docker(), "This is runnable only inside Docker.")
    #
    reconcile_create_dirs(ctx, run_date=run_date, dst_dir=dst_dir)
    #
    reconcile_copy_prod_data(
        ctx,
        run_date=run_date,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    #
    reconcile_dump_market_data(
        ctx,
        run_date=run_date,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    reconcile_run_sim(
        ctx,
        run_date=run_date,
        dst_dir=dst_dir,
        rt_timeout_in_secs_or_time=rt_timeout_in_secs_or_time,
    )
    reconcile_copy_sim_data(ctx, run_date=run_date, dst_dir=dst_dir)
    #
    reconcile_dump_tca_data(
        ctx,
        run_date=run_date,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    #
    if not skip_notebook:
        reconcile_run_notebook(
            ctx,
            run_date=run_date,
            dst_dir=dst_dir,
            prevent_overwriting=prevent_overwriting,
        )
    reconcile_ls(ctx, run_date=run_date, dst_dir=dst_dir)
