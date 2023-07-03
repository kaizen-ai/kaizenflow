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
> invoke reconcile_run_all --dag-builder-name "C1b" --run-mode "prod" --start-timestamp-as-str "20221017_063500" --end-timestamp-as-str "20221017_073500"
```

to run outside a Docker container:
```
> invoke docker_cmd --cmd 'invoke reconcile_run_all --dag-builder-name "C1b" --run-mode "prod" --start-timestamp-as-str "20221017_063500" --end-timestamp-as-str "20221017_073500"'
```

Import as:

import oms.lib_tasks_reconcile as olitarec
"""

import datetime
import logging
import os
import sys
from typing import Optional

from invoke import task

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hserver as hserver
import helpers.hsystem as hsystem
import oms.reconciliation as omreconc

_LOG = logging.getLogger(__name__)


def _system(cmd: str) -> int:
    return hsystem.system(cmd, suppress_output=False, log_level="echo")


def _allow_update(
    dag_builder_name: str,
    run_mode: str,
    start_timestamp_as_str: str,
    dst_root_dir: str,
) -> None:
    """
    Allow to overwrite reconcilation outcomes in the date-specific target dir.

    See `reconcile_run_all()` for params description.
    """
    # Get date-specific target dir.
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    hdbg.dassert_path_exists(target_dir)
    # Allow overwritting.
    _LOG.info("Allow to overwrite files at: %s", target_dir)
    cmd = f"chmod -R +w {target_dir}"
    _system(cmd)


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


# TODO(Nina): extend to handle S3 files.
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


# TODO(Grisha): seems more general than this file.
# TODO(Grisha): extend to handle dirs.
def _copy_result_file(
    file_path: str, dst_dir: str, aws_profile: Optional[str]
) -> None:
    """
    Copy a file to a specified dir.

    :param file_path: a path to copy data from
    :param dst_dir: a dir to copy data to
    """
    if hs3.is_s3_path(file_path):
        hs3.dassert_path_exists(file_path, aws_profile)
        cmd = f"aws s3 cp {file_path} {dst_dir} --profile {aws_profile}"
    else:
        hdbg.dassert_file_exists(file_path)
        hdbg.dassert_dir_exists(dst_dir)
        cmd = f"cp {file_path} {dst_dir}"
    _LOG.info("Copying from %s to %s", file_path, dst_dir)
    _system(cmd)


@task
def reconcile_create_dirs(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    dst_root_dir,
    abort_if_exists,
):  # type: ignore
    """
    Create dirs for storing reconciliation data.

    Final dirs layout is:
    ```
    {dst_root_dir}/
        {dag_builder_name}/
            {run_date}/
                {run_mode}/
                    prod/
                    simulation/
                    tca/
                    reconciliation_notebook/
                    ...
    ```

    See `reconcile_run_all()` for params description.

    :param abort_if_exists: see `hio.create_dir()`
    """
    _ = ctx
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    # Create a dir for reconcilation results.
    hio.create_dir(target_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Create dirs for storing prod and simulation results.
    prod_target_dir = omreconc.get_prod_dir(target_dir)
    sim_target_dir = omreconc.get_simulation_dir(target_dir)
    hio.create_dir(
        prod_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    hio.create_dir(
        sim_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dir for publishing reconciliation notebook.
    notebook_target_dir = omreconc.get_reconciliation_notebook_dir(target_dir)
    hio.create_dir(
        notebook_target_dir, incremental=True, abort_if_exists=abort_if_exists
    )
    # Create dir for dumped TCA data.
    if run_mode == "prod":
        # Transaction costs analysis is not applicable when running with a mocked
        # broker.
        tca_target_dir = omreconc.get_tca_dir(target_dir)
        hio.create_dir(
            tca_target_dir, incremental=True, abort_if_exists=abort_if_exists
        )
    # Sanity check the created dirs.
    cmd = f"ls -lh {target_dir}"
    _system(cmd)


@task
def reconcile_dump_market_data(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    end_timestamp_as_str,
    dst_root_dir,
    source_dir=None,
    incremental=False,
    interactive=False,
    prevent_overwriting=True,
    aws_profile=None,
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
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    sim_target_dir = omreconc.get_simulation_dir(target_dir)
    if source_dir is None:
        source_dir = "."
    market_data_file = "test_data.csv.gz"
    market_data_file_path = os.path.join(source_dir, market_data_file)
    if hs3.is_s3_path(source_dir):
        aws_profile = "ck"
        s3fs_ = hs3.get_s3fs(aws_profile)
        path_exists = s3fs_.exists(market_data_file_path)
    else:
        path_exists = os.path.exists(market_data_file)
    # TODO(Grisha): @Dan Reconsider clause logic (compare with `reconcile_run_notebook`).
    if incremental and path_exists:
        # Copy market data file to the target dir.
        _copy_result_file(market_data_file_path, sim_target_dir, aws_profile)
        _LOG.warning("Skipping generating %s", market_data_file_path)
    else:
        # TODO(Grisha): @Dan Copy logs to the specified folder.
        opts = [
            # TODO(Grisha): consider saving locally as it is done for the other files / dirs.
            f"--dst_dir {sim_target_dir}",
            f"--start_timestamp_as_str {start_timestamp_as_str}",
            f"--end_timestamp_as_str {end_timestamp_as_str}",
        ]
        opts = " ".join(opts)
        opts += " -v DEBUG 2>&1 | tee reconcile_dump_market_data_log.txt; exit ${PIPESTATUS[0]}"
        amp_dir = hgit.get_amp_abs_path()
        script_name = "dataflow_amp/system/Cx/Cx_dump_market_data.py"
        script_name = os.path.join(amp_dir, script_name)
        cmd = " ".join([script_name, opts])
        _system(cmd)
    # Check the market data file.
    # TODO(Nina): enable once the function is extended to handle S3 files.
    # _sanity_check_data(market_data_file_path)
    if interactive:
        question = "Is the file ok?"
        hsystem.query_yes_no(question)
    # Sanity check result data.
    market_data_file_target = os.path.join(sim_target_dir, market_data_file_path)
    # _sanity_check_data(market_data_file_target)
    #
    if prevent_overwriting:
        _prevent_overwriting(market_data_file_target)


@task
def reconcile_run_sim(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    end_timestamp_as_str,
    dst_root_dir,
):  # type: ignore
    """
    Run the simulation given an interval [start_timestamp, end_timestamp].

    See `reconcile_run_all()` for params description.
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    _ = ctx
    # TODO(Nina): consider to save directly to sim dir and not to a root dir;
    # or delete this dir inside `reconcile_copy_sim_data()`.
    local_results_dir = "system_log_dir"
    if os.path.exists(local_results_dir):
        rm_cmd = f"rm -rf {local_results_dir}"
        _LOG.warning(
            "The local_results_dir=%s already exists, removing it.",
            local_results_dir,
        )
        _system(rm_cmd)
    script_name = os.path.join(
        "dataflow_orange", "system", "Cx", "Cx_run_prod_system_simulation.py"
    )
    # Build market data file path.
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    sim_target_dir = omreconc.get_simulation_dir(target_dir)
    file_name = "test_data.csv.gz"
    market_data_file_path = os.path.join(sim_target_dir, file_name)
    system_log_dir = "./system_log_dir"
    # Run simulation.
    cmd_txt = [
        script_name,
        f"--dag_builder_name {dag_builder_name}",
        f"--start_timestamp_as_str {start_timestamp_as_str}",
        f"--end_timestamp_as_str {end_timestamp_as_str}",
        f"--market_data_file_path {market_data_file_path}",
        f"--dst_dir {system_log_dir}",
        "-v DEBUG 2>&1 | tee reconcile_run_sim_log.txt; exit ${PIPESTATUS[0]}",
    ]
    cmd = " ".join(cmd_txt)
    _system(cmd)
    # Check that the required dirs were created.
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "dag"))
    hdbg.dassert_dir_exists(os.path.join(local_results_dir, "process_forecasts"))
    # TODO(Grisha): @Dan Add asserts on the latest files so we confirm that simulation was completed.


@task
def reconcile_copy_sim_data(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    dst_root_dir,
    prevent_overwriting=True,
    aws_profile=None,
):  # type: ignore
    """
    Copy the output of the simulation run to the specified folder.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    sim_target_dir = omreconc.get_simulation_dir(target_dir)
    # Make sure that the destination dir exists before copying.
    hdbg.dassert_dir_exists(sim_target_dir)
    _LOG.info("Copying results to '%s'", sim_target_dir)
    # Copy the output to the specified folder.
    system_log_dir = "./system_log_dir"
    cmd = f"cp -vr {system_log_dir} {sim_target_dir}"
    _system(cmd)
    # Copy simulation run logs to the specified folder.
    pytest_log_file_path = "reconcile_run_sim_log.txt"
    _copy_result_file(pytest_log_file_path, sim_target_dir, aws_profile)
    if prevent_overwriting:
        _prevent_overwriting(sim_target_dir)


@task
def reconcile_copy_prod_data(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    end_timestamp_as_str,
    dst_root_dir,
    # TODO(Nina): -> "source_dir".
    prod_data_source_dir,
    mode,
    stage=None,
    prevent_overwriting=True,
    aws_profile=None,
):  # type: ignore
    """
    Copy the output of the prod run to the specified folder.

    See `reconcile_run_all()` for params description.
    """
    if stage is None:
        stage = "preprod"
    hdbg.dassert_in(stage, ("local", "test", "preprod", "prod"))
    hdbg.dassert_in(mode, ("scheduled", "manual"))
    hs3.dassert_path_exists(prod_data_source_dir, aws_profile)
    _ = ctx
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    # Set source log dir.
    system_log_subdir = omreconc.get_prod_system_log_dir(
        mode, start_timestamp_as_str, end_timestamp_as_str
    )
    system_log_dir = os.path.join(prod_data_source_dir, system_log_subdir)
    hs3.dassert_path_exists(system_log_dir, aws_profile)
    # Set target dirs.
    prod_target_dir = omreconc.get_prod_dir(target_dir)
    prod_results_target_dir = os.path.join(prod_target_dir, system_log_subdir)
    _LOG.info("Copying prod results to '%s'", prod_results_target_dir)
    # Copy prod run results to the target dir.
    if hs3.is_s3_path(system_log_dir):
        cmd = f"aws s3 cp {system_log_dir} {prod_results_target_dir} --recursive --profile {aws_profile}"
    else:
        cmd = f"cp -vr {system_log_dir} {prod_results_target_dir}"
    _system(cmd)
    # Copy prod run logs to the specified folder.
    log_file = f"log.{mode}.{start_timestamp_as_str}.{end_timestamp_as_str}.txt"
    log_file = os.path.join(prod_data_source_dir, "logs", log_file)
    #
    _copy_result_file(log_file, prod_target_dir, aws_profile)
    #
    if prevent_overwriting:
        _prevent_overwriting(prod_target_dir)


@task
def reconcile_run_notebook(
    ctx,
    notebook_run_mode,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    end_timestamp_as_str,
    dst_root_dir,
    mode,
    incremental=False,
    mark_as_last_24_hour_run=False,
    prevent_overwriting=True,
):  # type: ignore
    """
    Run the reconciliation notebook, publish it locally and copy the results to
    the specified folder.

    See `reconcile_run_all()` for params description.

    :param notebook_run_mode: same as in `get_system_reconciliation_notebook_path()`
    :param incremetal: see `hio.create_dir()`
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    hdbg.dassert_in(mode, ("scheduled", "manual"))
    _ = ctx
    # Set results destination dir and clear it if is already filled.
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    hdbg.dassert_dir_exists(target_dir)
    # The common pattern is to save an output locally and copy to the specified
    # folder. However, if a notebook run fails then the copying piece of the
    # code is not executed so saving the output directly to the specified folder.
    notebook_root_dir = omreconc.get_reconciliation_notebook_dir(target_dir)
    _LOG.debug(hprint.to_str("notebook_root_dir"))
    # Store the "fast" and the "slow" notebook results separately.
    notebook_dir = os.path.join(notebook_root_dir, notebook_run_mode)
    _LOG.debug(hprint.to_str("notebook_dir"))
    if os.path.exists(notebook_dir):
        if incremental:
            _LOG.warning(
                "Notebook run results are already stored at %s", notebook_dir
            )
            sys.exit(-1)
        else:
            rm_cmd = f"rm -rf {notebook_dir}"
            _LOG.warning(
                "The results_dir=%s already exists, removing it.", notebook_dir
            )
            _system(rm_cmd)
    # TODO(Grisha): pass `asset_class` as a param.
    asset_class = "crypto"
    #
    cmd_txt = []
    cmd_txt.append(f"export AM_ASSET_CLASS={asset_class}")
    # Add the command to run the notebook.
    amp_dir = hgit.get_amp_abs_path()
    script_path = os.path.join(
        amp_dir, "dev_scripts", "notebooks", "run_notebook.py"
    )
    notebook_path = omreconc.get_system_reconciliation_notebook_path(
        notebook_run_mode
    )
    config_builder = (
        f"amp.oms.reconciliation.build_reconciliation_configs"
        + f'("{dst_root_dir}", "{dag_builder_name}", "{start_timestamp_as_str}", "{end_timestamp_as_str}", "{run_mode}", "{mode}")'
    )
    opts = "--tee --no_suppress_output --num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt; exit ${PIPESTATUS[0]}"
    cmd_run_txt = [
        script_path,
        f"--notebook {notebook_path}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir {notebook_dir}",
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
    # Copy the notebook on S3 HTML bucket so that it is accessible via
    # a web-browser.
    html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
    s3_dst_dir = os.path.join(html_bucket_path, "system_reconciliation")
    #
    notebook_name = os.path.basename(notebook_path)
    notebook_name = os.path.splitext(notebook_name)[0]
    cmd = f"find {notebook_dir} -type f -name '{notebook_name}.*.html'"
    _, html_notebook_path = hsystem.system_to_string(cmd)
    html_notebook_name = os.path.basename(html_notebook_path)
    #
    s3_dst_path = os.path.join(s3_dst_dir, html_notebook_name)
    aws_profile = "ck"
    hs3.copy_file_to_s3(html_notebook_path, s3_dst_path, aws_profile)
    if mark_as_last_24_hour_run:
        # Copy the reconciliation notebook to S3 HTML bucket so that it becomes
        # available via the static link, e.g.,
        # `https://***/system_reconciliation/C3a.last_24hours.html`.
        html_file_name = (
            f"{dag_builder_name}.{notebook_run_mode}.last_24hours.html"
        )
        s3_dst_path = os.path.join(s3_dst_dir, html_file_name)
        hs3.copy_file_to_s3(html_notebook_path, s3_dst_path, aws_profile)
    if prevent_overwriting:
        _prevent_overwriting(notebook_dir)


# TODO(Grisha): consider moving to a different file.
@task
def run_master_pnl_real_time_observer_notebook(
    ctx,
    prod_data_root_dir,
    dag_builder_name,
    start_timestamp_as_str,
    end_timestamp_as_str,
    mode,
    burn_in_bars=3,
    mark_as_last_5minute_run=False,
):  # type: ignore
    """
    Run the PnL real time observer notebook and copy the results to the S3 HTML
    bucket.

    :param prod_data_root_dir: a dir that stores the production system output,
        e.g., "/shared_data/ecs/preprod/system_reconciliation/"
    :param dag_builder_name: name of the DAG builder, e.g. "C1b"
    :param start_timestamp_as_str: string representation of timestamp
        at which a production run started
    :param end_timestamp_as_str: string representation of timestamp
        at which a production run ended
    :param mode: the prod system run mode which defines a prod system log dir name
        - "scheduled": the system is run at predefined time automatically
        - "manual": the system run is triggered manually
    :param burn_in_bars: a minimum amount of bars that is needed for calculations
    :param mark_as_last_5minute_run: mark the production run as latest so
        that the latest PnL real-time observer notebook becomes available via
        a static link, e.g., `https://***/system_reconciliation/C3a.last_5minutes.html`
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    hdbg.dassert_in(mode, ("scheduled", "manual"))
    hdbg.dassert_dir_exists(prod_data_root_dir)
    # Check if at least `burn_in_bars` bars are computed, otherwise exit.
    # TODO(Grisha): consider passing the system_log_dir path directly to the
    # config building function.
    run_date = omreconc.get_run_date(start_timestamp_as_str)
    target_dir = os.path.join(prod_data_root_dir, dag_builder_name, run_date)
    system_log_dir = omreconc.get_prod_system_log_dir(
        mode, start_timestamp_as_str, end_timestamp_as_str
    )
    system_log_dir = os.path.join(target_dir, system_log_dir)
    data_type = "orders"
    orders_dir = omreconc.get_data_type_system_log_path(system_log_dir, data_type)
    # TODO(Grisha): create a separate function to get orders files.
    order_files = omreconc._get_dag_node_csv_file_names(orders_dir)
    # There is a separate order file for each bar. Assumption: a bar is computed
    # when a corresponding order file exists. We use the number of order files as
    # a proxy for the number of computed bars.
    n_bars = len(order_files)
    if n_bars < burn_in_bars:
        _LOG.warn(
            "Not enough data to run the notebook, should be at least %s bars computed, received %s bars, exiting",
            burn_in_bars,
            n_bars,
        )
        return None
    _ = ctx
    # Add the command to run the notebook.
    amp_dir = hgit.get_amp_abs_path()
    notebook_name = "Master_PnL_real_time_observer"
    script_path = os.path.join(
        amp_dir, "dev_scripts", "notebooks", "run_notebook.py"
    )
    notebook_path = os.path.join(
        amp_dir, "oms", "notebooks", f"{notebook_name}.ipynb"
    )
    save_plots_for_investors = True
    html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
    s3_dst_dir = os.path.join(html_bucket_path, "pnl_for_investors")
    config_builder = (
        f"amp.oms.reconciliation.build_prod_pnl_real_time_observer_configs"
        + f'("{prod_data_root_dir}", "{dag_builder_name}", "{start_timestamp_as_str}", "{end_timestamp_as_str}",  "{mode}", {save_plots_for_investors}, s3_dst_dir="{s3_dst_dir}")'
    )
    # Since the invoke is run multiple times per day create a subdir for every
    # run and mark it with the current UTC timestamp, e.g.,
    # `.../C3a/20230411/pnl_realtime_observer_notebook/20230411_130510`.
    run_date = omreconc.get_run_date(start_timestamp_as_str)
    notebook_dir = "pnl_realtime_observer_notebook"
    shared_notebook_dir = os.path.join(
        prod_data_root_dir, dag_builder_name, run_date, notebook_dir
    )
    hio.create_dir(shared_notebook_dir, incremental=True)
    #
    tz = "UTC"
    current_time = hdateti.get_current_timestamp_as_string(tz)
    current_time = current_time.replace("-", "_")
    # E.g., `.../C3a/20230411/pnl_realtime_observer_notebook/20230411_130510`.
    shared_nootebook_timestamp_dir = os.path.join(
        shared_notebook_dir, current_time
    )
    hio.create_dir(shared_nootebook_timestamp_dir, incremental=True)
    opts = "--tee --no_suppress_output --num_threads 'serial' --publish_notebook -v DEBUG 2>&1 | tee log.txt; exit ${PIPESTATUS[0]}"
    cmd_run_txt = [
        script_path,
        f"--notebook {notebook_path}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir {shared_nootebook_timestamp_dir}",
        f"{opts}",
    ]
    cmd_txt = " ".join(cmd_run_txt)
    # Save the commands as a script.
    script_name = "tmp.publish_notebook.sh"
    hio.create_executable_script(script_name, cmd_txt)
    # Run the notebook.
    _LOG.info("Running the notebook=%s", notebook_path)
    _system(script_name)
    # Remove the tmp script.
    rm_cmd = f"rm {script_name}"
    _system(rm_cmd)
    if mark_as_last_5minute_run:
        # Search for the published notebook.
        cmd = f"find {shared_nootebook_timestamp_dir} -type f -name '{notebook_name}.*.html'"
        _, html_notebook_path = hsystem.system_to_string(cmd)
        # Copy the notebook to the S3 HTML bucket so that
        # it becomes available via the static link, e.g.,
        # `https://***/system_reconciliation/C3a.last_5minutes.html`.
        html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
        html_file_name = f"{dag_builder_name}.last_5minutes.html"
        s3_dst_path = os.path.join(
            html_bucket_path, "system_reconciliation", html_file_name
        )
        aws_profile = "ck"
        hs3.copy_file_to_s3(html_notebook_path, s3_dst_path, aws_profile)


@task
def reconcile_ls(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    dst_root_dir,
):  # type: ignore
    """
    Run `ls` on the dir containing the reconciliation data.

    See `reconcile_run_all()` for params description.
    """
    _ = ctx
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
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
    dag_builder_name,
    start_timestamp_as_str,
    dst_root_dir,
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
    # TCA is being dumped only for prod run mode.
    run_mode = "prod"
    target_dir = omreconc.get_target_dir(
        dst_root_dir, dag_builder_name, start_timestamp_as_str, run_mode
    )
    run_date = omreconc.get_run_date(start_timestamp_as_str)
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
        "amp/oms/ccxt/scripts/get_ccxt_trades.py",
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
        tca_dir = omreconc.get_tca_dir(target_dir)
        _prevent_overwriting(tca_dir)


@task
def reconcile_run_all(
    ctx,
    dag_builder_name,
    run_mode,
    start_timestamp_as_str,
    end_timestamp_as_str,
    dst_root_dir,
    prod_data_source_dir,
    mode,
    market_data_source_dir=None,
    stage=None,
    mark_as_last_24_hour_run=False,
    # TODO(Grisha): propagate everywhere below.
    incremental=False,
    prevent_overwriting=True,
    abort_if_exists=False,
    run_notebook=False,
    allow_update=False,
    aws_profile=None,
):  # type: ignore
    """
    Run all phases of prod vs simulation reconciliation.

    :param dag_builder_name: name of the DAG builder, e.g. "C1b"
    :param run_mode: prod run mode, e.g. "prod" or "paper_trading"
    :param start_timestamp_as_str: string representation of timestamp
        at which to start reconcile run
    :param end_timestamp_as_str: string representation of timestamp
        at which to end reconcile run
    :param dst_root_dir: dir to store reconcilation results in
    :param prod_data_source_dir: path to the prod run outcomes
        (i.e. system log dir, logs)
    :param stage: development stage, e.g., `preprod`
    :param mark_as_last_24_hour_run: mark the reconciliation run as latest so
        that the latest reconciliation notebook becomes available via a static
        link, e.g., `https://***/system_reconciliation/C3a.last_24hours.html`
    :param mode: the prod system run mode which defines a prod system log dir name
        - "scheduled": the system is run at predefined time automatically
        - "manual": the system run is triggered manually
    :param prevent_overwriting: if True write permissions are removed otherwise
        a permissions remain as they are
    :param abort_if_exists: see `hio.create_dir()`
    :param run_notebook: if True run the reconcilation notebook otherwise do not run
    :param allow_update: if True allow to overwrite reconcilation outcomes
        otherwise retain permissions as they are
    :param aws_profile: AWS profile, e.g., "ck"
    """
    hdbg.dassert(
        hserver.is_inside_docker(), "This is runnable only inside Docker."
    )
    #
    reconcile_create_dirs(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        dst_root_dir,
        abort_if_exists,
    )
    #
    reconcile_copy_prod_data(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        end_timestamp_as_str,
        dst_root_dir,
        prod_data_source_dir,
        mode,
        stage=stage,
        prevent_overwriting=prevent_overwriting,
        aws_profile=aws_profile,
    )
    #
    reconcile_dump_market_data(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        end_timestamp_as_str,
        dst_root_dir,
        source_dir=market_data_source_dir,
        incremental=incremental,
        prevent_overwriting=prevent_overwriting,
    )
    reconcile_run_sim(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        end_timestamp_as_str,
        dst_root_dir,
    )
    reconcile_copy_sim_data(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        dst_root_dir,
        prevent_overwriting=prevent_overwriting,
    )
    # TODO(Grisha): decide do we need to run TCA given that `CcxtBroker` logs
    # the fills.
    # if run_mode == "prod":
    #     reconcile_dump_tca_data(
    #         ctx,
    #         dag_builder_name,
    #         start_timestamp_as_str,
    #         dst_root_dir,
    #         prevent_overwriting=prevent_overwriting,
    #     )
    if run_notebook:
        notebook_run_mode = "fast"
        reconcile_run_notebook(
            ctx,
            notebook_run_mode,
            dag_builder_name,
            run_mode,
            start_timestamp_as_str,
            end_timestamp_as_str,
            dst_root_dir,
            mode,
            mark_as_last_24_hour_run=mark_as_last_24_hour_run,
            prevent_overwriting=prevent_overwriting,
        )
        notebook_run_mode = "slow"
        reconcile_run_notebook(
            ctx,
            notebook_run_mode,
            dag_builder_name,
            run_mode,
            start_timestamp_as_str,
            end_timestamp_as_str,
            dst_root_dir,
            mode,
            mark_as_last_24_hour_run=mark_as_last_24_hour_run,
            prevent_overwriting=prevent_overwriting,
        )
    reconcile_ls(
        ctx,
        dag_builder_name,
        run_mode,
        start_timestamp_as_str,
        dst_root_dir,
    )
    if allow_update:
        _allow_update(
            dag_builder_name,
            run_mode,
            start_timestamp_as_str,
            dst_root_dir,
        )
