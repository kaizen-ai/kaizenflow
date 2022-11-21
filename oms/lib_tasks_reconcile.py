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
# TODO(Grisha): do we need TCA?
# 7) Dump TCA data
# 8) Run the reconciliation notebook and publish it

"""
Invokes in the file are runnable from a Docker container only.

E.g., to run for certain date from a Docker container:
```
> invoke run_reconcile_run_all --start-timestamp-as-str "20221017_063500"
```

to run outside a Docker container:
```
> invoke docker_cmd --cmd 'invoke run_reconcile_run_all --start-timestamp-as-str "20221017_063500"'
```

Import as:

import dev_scripts.lib_tasks_reconcile as dslitare
"""

import datetime
import logging
import os
import sys
from typing import Optional, Tuple

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
    """
    Check if an input string is a date.

    :param date: date as string, e.g., "20221101"
    """
    hdbg.dassert_isinstance(date, str)
    try:
        _ = datetime.datetime.strptime(date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"date='{date}' doesn't have the right format: {e}")


def _get_run_date(start_timestamp_as_str: Optional[str]) -> str:
    """
    Return the run date as string from start timestamp, e.g. "20221017".

    If start timestamp is not specified by a user then return current
    date.

    E.g., "20221101_064500" -> "20221101".
    """
    if start_timestamp_as_str is None:
        run_date = datetime.date.today().strftime("%Y%m%d")
    else:
        # TODO(Dan): Add assert for `start_timestamp_as_str` regex.
        run_date = start_timestamp_as_str.split("_")[0]
    _LOG.info(hprint.to_str("run_date"))
    _dassert_is_date(run_date)
    return run_date


def _prevent_overwriting(path: str) -> None:
    """
    Remove write permissions.

    :param path: path to a file or to a dir
    """
    hdbg.dassert_path_exists(path)
    _LOG.info("Removing the write permissions for: %s", path)
    if os.path.isdir(path):
        opt = "-R"
    else:
        opt = ""
    cmd = f"chmod {opt} -w {path}"
    _system(cmd)


def _resolve_target_dir(run_date: str, dst_dir: Optional[str]) -> str:
    """
    Return the target dir name to store reconcilation results.

    If a dir name is not specified by a user then use prod reconcilation
    dir on the shared disk with the corresponding run date subdir.

    E.g., "/shared_data/prod_reconciliation/20221101".

    # TODO(Grisha): use `root_dir` everywhere, for a date specific dir use `dst_dir`.
    :param run_date: string representation of the reconcile run date
    :param dst_dir: a root dir for prod system reconciliation
    :return: a target dir to store reconcilation results
    """
    dst_dir = dst_dir or _PROD_RECONCILIATION_DIR
    target_dir = os.path.join(dst_dir, run_date)
    _LOG.info(hprint.to_str("target_dir"))
    return target_dir


def _resolve_timestamps(
    start_timestamp_as_str: Optional[str], end_timestamp_as_str: Optional[str]
) -> Tuple[str, str]:
    """
    Return start and end timestamps.

    If a timestamps is not specified by a user then set a default value for it
    and return it.
    """
    today_as_str = datetime.date.today().strftime("%Y%m%d")
    if start_timestamp_as_str is None:
        start_timestamp_as_str = "_".join([today_as_str, "060500"])
    if end_timestamp_as_str is None:
        end_timestamp_as_str = "_".join([today_as_str, "080000"])
    return start_timestamp_as_str, end_timestamp_as_str


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
    ctx, start_timestamp_as_str=None, dst_dir=None, abort_if_exists=True
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
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
    # Create a dir for reconcilation results.
    hio.create_dir(target_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Create dirs for storing prod and simulation results.
    prod_target_dir = os.path.join(target_dir, "prod")
    sim_target_dir = os.path.join(target_dir, "simulation")
    hio.create_dir(
        prod_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    hio.create_dir(
        sim_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dir for dumped TCA data.
    tca_target_dir = os.path.join(target_dir, "tca")
    hio.create_dir(
        tca_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Sanity check the created dirs.
    cmd = f"ls -lh {target_dir}"
    _system(cmd)


@task
def reconcile_dump_market_data(
    ctx,
    start_timestamp_as_str=None,
    end_timestamp_as_str=None,
    dst_dir=None,
    incremental=False,
    interactive=False,
    prevent_overwriting=True,
):  # type: ignore
    # pylint: disable=line-too-long
    """
    Dump the market data image and copy it to the specified folder.

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
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    start_timestamp_as_str, end_timestamp_as_str = _resolve_timestamps(
        start_timestamp_as_str, end_timestamp_as_str
    )
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
    market_data_file = "test_data.csv.gz"
    # TODO(Grisha): @Dan Reconsider clause logic (compare with `reconcile_run_notebook`).
    if incremental and os.path.exists(market_data_file):
        _LOG.warning("Skipping generating %s", market_data_file)
    else:
        # TODO(Grisha): @Dan Copy logs to the specified folder.
        # TODO(Grisha): @Dan Remove unnecessary opts.
        opts = [
            "--action dump_data",
            f"--start_timestamp_as_str {start_timestamp_as_str}",
            f"--end_timestamp_as_str {end_timestamp_as_str}",
            f"--dst_dir {dst_dir}",
        ]
        opts = " ".join(opts)
        opts += " -v DEBUG 2>&1 | tee reconcile_dump_market_data_log.txt; exit ${PIPESTATUS[0]}"
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
        _prevent_overwriting(market_data_file_target)


@task
def reconcile_run_sim(
    ctx,
    start_timestamp_as_str=None,
    end_timestamp_as_str=None,
    dst_dir=None,
):  # type: ignore
    """
    Run the simulation given an interval [start_timestamp, end_timestamp].

    See `reconcile_run_all()` for params description.
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    start_timestamp_as_str, end_timestamp_as_str = _resolve_timestamps(
        start_timestamp_as_str, end_timestamp_as_str
    )
    # TODO(Grisha): maybe include date for a default value? i.e. `.../20221101`.
    dst_dir = dst_dir or _PROD_RECONCILIATION_DIR
    local_results_dir = "system_log_dir"
    if os.path.exists(local_results_dir):
        rm_cmd = f"rm -rf {local_results_dir}"
        _LOG.warning(
            "The local_results_dir=%s already exists, removing it.",
            local_results_dir,
        )
        _system(rm_cmd)
    # Run simulation.
    opts = [
        "--action run_simulation",
        f"--start_timestamp_as_str {start_timestamp_as_str}",
        f"--end_timestamp_as_str {end_timestamp_as_str}",
        f"--dst_dir {dst_dir}",
    ]
    opts = " ".join(opts)
    opts += (
        " -v DEBUG 2>&1 | tee reconcile_run_sim_log.txt; exit ${PIPESTATUS[0]}"
    )
    script_name = "dataflow_orange/system/C1/C1b_reconcile.py"
    cmd = f"{script_name} {opts}"
    _system(cmd)
    # Check that the required dirs were created.
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "process_forecasts"))
    # TODO(Grisha): @Dan Add asserts on the latest files so we confirm that simulation was completed.


@task
def reconcile_copy_sim_data(
    ctx, start_timestamp_as_str=None, dst_dir=None, prevent_overwriting=True
):  # type: ignore
    """
    Copy the output of the simulation run to the specified folder.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
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
        _prevent_overwriting(sim_target_dir)


@task
def reconcile_copy_prod_data(
    ctx,
    start_timestamp_as_str=None,
    end_timestamp_as_str=None,
    dst_dir=None,
    stage=None,
    mode=None,
    prevent_overwriting=True,
):  # type: ignore
    """
    Copy the output of the prod run to the specified folder.

    See `reconcile_run_all()` for params description.

    :param stage: development stage, e.g., `preprod`
    :param mode: the prod system run mode which defines a prod system log dir name
        - "scheduled": the system is run at predefined time automatically
        - "manual": the system run is triggered manually
    """
    # Moved inside the function due to `oms` dependency. See CMTask #3151.
    import oms

    start_timestamp_as_str, end_timestamp_as_str = _resolve_timestamps(
        start_timestamp_as_str, end_timestamp_as_str
    )
    if stage is None:
        stage = "preprod"
    if mode is None:
        mode = "scheduled"
    hdbg.dassert_in(stage, ("local", "test", "preprod", "prod"))
    hdbg.dassert_in(mode, ("scheduled", "manual"))
    _ = ctx
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
    prod_target_dir = os.path.join(target_dir, "prod")
    # Make sure that the target dir exists before copying.
    hdbg.dassert_dir_exists(prod_target_dir)
    _LOG.info("Copying results to '%s'", prod_target_dir)
    # Copy prod run results to the target dir.
    shared_dir = f"/shared_data/ecs/{stage}/system_reconciliation"
    system_log_dir = oms.get_prod_system_log_dir(
        mode, start_timestamp_as_str, end_timestamp_as_str
    )
    system_log_dir = os.path.join(shared_dir, system_log_dir)
    hdbg.dassert_dir_exists(system_log_dir)
    cmd = f"cp -vr {system_log_dir} {prod_target_dir}"
    _system(cmd)
    # Copy prod run logs to the specified folder.
    log_file = f"log_{mode}.{start_timestamp_as_str}.{end_timestamp_as_str}.txt"
    log_file = os.path.join(shared_dir, "logs", log_file)
    hdbg.dassert_file_exists(log_file)
    cmd = f"cp -v {log_file} {prod_target_dir}"
    _system(cmd)
    #
    if prevent_overwriting:
        _prevent_overwriting(prod_target_dir)


# TODO(Grisha): @Dan Expose `start_timestamp_as_str` and `start_timestamp_as_str` use in the notebook.
@task
def reconcile_run_notebook(
    ctx,
    start_timestamp_as_str=None,
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
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    run_date = _get_run_date(start_timestamp_as_str)
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
    prod_subdir = None
    # pylint: disable=line-too-long
    config_builder = f'amp.oms.reconciliation.build_reconciliation_configs(date_str="{run_date}", prod_subdir={prod_subdir})'
    # pylint: enable=line-too-long
    opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt; exit ${PIPESTATUS[0]}"
    cmd_run_txt = [
        "amp/dev_scripts/notebooks/run_notebook.py",
        f"--notebook {notebook_path}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir {local_results_dir}",
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
    _system(script_name)
    # Copy the published notebook to the specified folder.
    hdbg.dassert_dir_exists(results_dir)
    target_dir = _resolve_target_dir(run_date, dst_dir)
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    cmd = f"cp -vr {results_dir} {target_dir}"
    _system(cmd)
    #
    if prevent_overwriting:
        results_target_dir = os.path.join(target_dir, "result_0")
        _prevent_overwriting(results_target_dir)


@task
def reconcile_ls(ctx, start_timestamp_as_str=None, dst_dir=None):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
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
    start_timestamp_as_str=None,
    dst_dir=None,
    incremental=False,
    prevent_overwriting=True,
):  # type: ignore
    """
    Retrieve and save the TCA data.

    See `reconcile_run_all()` for params description.

    :param incremetal: see `hio.create_dir()`
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    run_date = _get_run_date(start_timestamp_as_str)
    target_dir = _resolve_target_dir(run_date, dst_dir)
    run_date = datetime.datetime.strptime(run_date, "%Y%m%d")
    # TODO(Grisha): add as params to the interface.
    end_timestamp = run_date
    start_timestamp = (end_timestamp - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )
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
    opts = [
        f"--exchange_id {exchange_id}",
        f"--contract_type {contract_type}",
        f"--stage {stage}",
        f"--account_type {account_type}",
        f"--secrets_id {secrets_id}",
        f"--universe {universe}",
    ]
    opts = " ".join(opts)
    log_file = os.path.join(local_results_dir, "log.txt")
    opts += f" --incremental -v DEBUG 2>&1 | tee {log_file}"
    opts += "; exit ${PIPESTATUS[0]}"
    cmd_run_txt = [
        "amp/oms/get_ccxt_fills.py",
        f"--start_timestamp '{start_timestamp}'",
        f"--end_timestamp '{end_timestamp}'",
        f"--dst_dir {local_results_dir}",
        f"{opts}",
    ]
    cmd_run_txt = " ".join(cmd_run_txt)
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
        tca_dir = os.path.join(target_dir, "tca")
        _prevent_overwriting(tca_dir)


@task
def reconcile_run_all(
    ctx,
    start_timestamp_as_str=None,
    end_timestamp_as_str=None,
    dst_dir=None,
    stage=None,
    mode=None,
    prevent_overwriting=True,
    skip_notebook=False,
):  # type: ignore
    """
    Run all phases of prod vs simulation reconciliation.

    :param start_timestamp_as_str: string representation of timestamp
        at which to start reconcile run
    :param end_timestamp_as_str: string representation of timestamp
        at which to end reconcile run
    :param dst_dir: dir to store reconcilation results in
    :param mode: see `reconcile_copy_prod_data()`
    :param prevent_overwriting: if True write permissions are remove otherwise
        a permissions remain as they are
    :param skip_notebook: if True do not run the reconcilation notebook otherwise run
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    #
    reconcile_create_dirs(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        dst_dir=dst_dir,
    )
    #
    reconcile_copy_prod_data(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        end_timestamp_as_str=end_timestamp_as_str,
        dst_dir=dst_dir,
        stage=stage,
        mode=mode,
        prevent_overwriting=prevent_overwriting,
    )
    #
    reconcile_dump_market_data(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        end_timestamp_as_str=end_timestamp_as_str,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    reconcile_run_sim(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        end_timestamp_as_str=end_timestamp_as_str,
        dst_dir=dst_dir,
    )
    reconcile_copy_sim_data(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    reconcile_dump_tca_data(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        dst_dir=dst_dir,
        prevent_overwriting=prevent_overwriting,
    )
    #
    if not skip_notebook:
        reconcile_run_notebook(
            ctx,
            start_timestamp_as_str=start_timestamp_as_str,
            dst_dir=dst_dir,
            prevent_overwriting=prevent_overwriting,
        )
    reconcile_ls(
        ctx,
        start_timestamp_as_str=start_timestamp_as_str,
        dst_dir=dst_dir,
    )
