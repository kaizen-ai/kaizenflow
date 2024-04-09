"""
Invokes in the file are runnable from a Docker container only.

Examples:
```
docker> invoke run_notebooks \
        --system-log-dir "/shared_data/ecs/test/twap_experiment/20230814_1" \
        --base-dst-dir "s3://cryptokaizen-html/notebooks" \
        --system-log-dir-recon "/shared_data/ecs/preprod/prod_reconciliation/"

to run outside a Docker container:
```
> invoke docker_cmd --cmd 'invoke run_notebooks ...'
```

Import as:

import dev_scripts.lib_tasks_run_model_experiment_notebooks as dsltrmeno
"""

import logging
import os

from invoke import task

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsystem as hsystem
import im_v2.common.universe as ivcu
import reconciliation.sim_prod_reconciliation as rsiprrec

_LOG = logging.getLogger(__name__)


def _run_notebook(
    config_builder: str,
    base_dst_dir: str,
    notebook_path: str,
    abort_on_error=False,
) -> None:
    """
    Run analysis notebooks and store it in a specified location.

    :param base_dst_dir: top most directory to store data into
    :param notebook_path: relative path to the notebook to execute,
        assuming amp is a submodule.
    """
    # TODO(Juraj): this does not work in the cmamp prod container when ran
    #  via AWS ECS.
    # hdbg.dassert(
    #    hserver.is_inside_docker(), "This is runnable only inside Docker."
    # )
    # Set directory to store results locally.
    results_dir = "."
    cmd_txt = []
    opts = "--tee --no_suppress_output --num_threads 'serial' "
    if abort_on_error:
        opts += " --allow_errors"
    opts += (
        " --publish_notebook -v DEBUG 2>&1 | tee log.txt; exit ${PIPESTATUS[0]}"
    )
    cmd_run_txt = [
        "amp/dev_scripts/notebooks/run_notebook.py",
        f"--notebook {notebook_path}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir '{results_dir}'",
        f"{opts}",
    ]
    cmd_run_txt = " ".join(cmd_run_txt)
    cmd_txt.append(cmd_run_txt)
    cmd_txt = "\n".join(cmd_txt)
    # Save the commands as a script.
    script_name = "tmp.publish_notebook.sh"
    hio.create_executable_script(script_name, cmd_txt)
    # Delete the temp dir before execution.
    results_dir = os.path.join(results_dir, "result_0")
    hio.delete_dir(results_dir)
    # Make the script executable and run it.
    _LOG.info("Running the notebook=%s", notebook_path)
    # if allow_errors is True and we still encounter error then abort to prevent infinite loop.
    rc = hsystem.system(
        script_name,
        suppress_output=False,
        log_level="echo",
        abort_on_error=abort_on_error,
    )
    if rc != 0:
        _LOG.info(
            "Failed running notebook=%s, Trying again with --allow-errors",
            notebook_path,
        )
        _run_notebook(
            config_builder, base_dst_dir, notebook_path, abort_on_error=True
        )
        return
    # Move the ipynb.html file to s3.
    if hs3.is_s3_path(base_dst_dir):
        notebook_name = os.path.basename(notebook_path)
        notebook_name = os.path.splitext(notebook_name)[0]
        find_and_move_html_file_to_s3(notebook_name, results_dir, base_dst_dir)
    else:
        hio.create_dir(base_dst_dir, incremental=True)
        hdbg.dassert_dir_exists(base_dst_dir)
        _LOG.info("Copying results from '%s' to '%s'", results_dir, base_dst_dir)
        hsystem.system(script_name, suppress_output=False, log_level="echo")
    # Delete the temp dir
    hio.delete_dir(results_dir)


def find_and_move_html_file_to_s3(
    notebook_name: str, results_dir: str, base_dst_dir: str
) -> None:
    """
    Find the notebook_name.*.html file in results_dir and move to s3.

    :param notebook_name: html notebook name which needs to be publish
    :param results_dir: dir where html notebook is saved
    :param base_dst_dir: s3 dst path to publish
    """
    html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
    cmd = f"find {results_dir} -type f -name '{notebook_name}.*.html'"
    _, html_notebook_path = hsystem.system_to_string(cmd)
    html_notebook_name = os.path.basename(html_notebook_path)
    #
    s3_dst_path = os.path.join(base_dst_dir, html_notebook_name)
    aws_profile = "ck"
    hs3.copy_file_to_s3(html_notebook_path, s3_dst_path, aws_profile)
    if base_dst_dir.startswith(html_bucket_path):
        dir_to_url = henv.execute_repo_config_code(
            "get_html_dir_to_url_mapping()"
        )
        url_bucket_path = dir_to_url[html_bucket_path]
        url = s3_dst_path.replace(html_bucket_path, url_bucket_path)
        cmd = f"""
        # To open the notebook from a web-browser open a link:
        {url}
        """
        print(hprint.dedent(cmd))


def publish_system_reconciliation_notebook(
    system_log_dir: str, base_dst_dir: str
) -> None:
    """
    system_reconciliation notebook is already created with `reconcile_run_all`
    just need to publish it.

    :param system_log_dir: refer `run_notebooks()` for param description
    :param base_dst_dir: refer `run_notebooks()` for param description
    """
    reconciliation_notebook_path = system_log_dir.replace(
        "system_reconciliation", "prod_reconciliation"
    )
    # Filter out path , eg.: */prod_reconciliation/C5b/prod/20240108_170500.20240108_173000/system_log_dir.manual/process_forecasts
    # -> */prod_reconciliation/C5b/prod/20240108_170500.20240108_173000
    reconciliation_notebook_path = "/".join(
        reconciliation_notebook_path.split("/")[:-2]
    )
    reconciliation_notebook_path = os.path.join(
        reconciliation_notebook_path, "reconciliation_notebook/fast/result_0"
    )
    notebook_name = "Master_system_reconciliation_fast"
    find_and_move_html_file_to_s3(
        notebook_name, reconciliation_notebook_path, base_dst_dir
    )


# TODO(Grisha): Consider to move the script to `oms.`
@task
def run_notebooks(
    ctx,
    system_log_dir,
    base_dst_dir,
):  # type: ignore
    """
    Run cross dataset reconciliation notebook and store it in a specified
    location.

    :param system_log_dir: dir where run logs are saved
        Eg.:
        ```
        Broker only :
            /shared_data/ecs/test/20240110_experiment1
        Full System run:
            /shared_data/ecs/test/system_reconciliation/C5b/prod/20240108_170500.20240108_173000/system_log_dir.manual/process_forecasts
        ```
    :param base_dst_dir: dir to store ipython notebooks
    """
    _ = ctx
    # `system_log_dir` is in a form `.../system_log_dir/process_forecasts` and
    # we want to extract `.../system_log_dir` to find SystemConfig.
    full_system_log_dir, _ = os.path.split(system_log_dir)
    # TODO(Sonaal): get the `system_config.output.values_as_strings.pkl` from
    # some constant.
    config_path = os.path.join(
        full_system_log_dir, "system_config.output.values_as_strings.pkl"
    )
    # The assumption is that a full System run saves SystemConfig which is not
    # the case for broker-only runs.
    is_full_system_run = os.path.exists(config_path)
    # For broker only runs we are guaranteed to capture all bid/ask data
    # during experiment itself.
    bid_ask_data_source = "logged_during_experiment"
    if is_full_system_run:
        # Load pickled SystemConfig.
        config_file_name = "system_config.output.values_as_strings.pkl"
        system_config_path = os.path.join(full_system_log_dir, config_file_name)
        system_config = cconfig.load_config_from_pickle(system_config_path)
        hdbg.dassert_in("dag_runner_config", system_config)
        if isinstance(system_config["dag_runner_config"], tuple):
            _LOG.warning("Reading Config v1.0")
            bar_duration = rsiprrec.extract_bar_duration_from_pkl_config(
                full_system_log_dir
            )
            universe_version = rsiprrec.extract_universe_version_from_pkl_config(
                full_system_log_dir
            )
            child_order_execution_freq = (
                rsiprrec.extract_execution_freq_from_pkl_config(
                    full_system_log_dir
                )
            )
        else:
            # TODO(Grisha): preserve types when reading SystemConfig back and
            #  remove all the post-processing.
            _LOG.warning("Reading Config v2.0")
            hdbg.dassert_isinstance(system_config, cconfig.Config)
            # Extract bar duration in seconds from a loaded system config.
            bar_duration_in_secs = system_config["dag_runner_config"][
                "bar_duration_in_secs"
            ]
            # Convert to a string representation of bar duration in minutes
            # in order to use it in the pipeline.
            bar_duration_in_mins = int(bar_duration_in_secs / 60)
            bar_duration = f"{bar_duration_in_mins}T"
            universe_version = system_config["market_data_config"][
                "im_client_config"
            ]["universe_version"]
            child_order_execution_freq = system_config[
                "process_forecasts_node_dict"
            ]["process_forecasts_dict"]["order_config"]["execution_frequency"]
        _LOG.debug("Using bar_duration=%s from SystemConfig", bar_duration)
        _LOG.debug(
            "Using universe_version=%s from SystemConfig", universe_version
        )
        _LOG.debug(
            "Using child_order_execution_freq=%s from SystemConfig",
            child_order_execution_freq,
        )
        # For full system runs, the default method is to use bid/ask data
        # Logged after the experiment to avoid potential gaps.
        bid_ask_data_source = "logged_after_experiment"
    else:
        args_logfile = os.path.join(system_log_dir, "args.json")
        hdbg.dassert_file_exists(args_logfile)
        args_dict = hio.from_json(args_logfile)
        hdbg.dassert_in("parent_order_duration_in_min", args_dict.keys())
        bar_duration = str(args_dict["parent_order_duration_in_min"]) + "T"
        _LOG.debug("Using bar_duration from Broker only args")
        hdbg.dassert_in("universe", args_dict.keys())
        universe_version = args_dict["universe"]
        _LOG.debug("Using universe_version from Broker only args")
        hdbg.dassert_in("child_order_execution_freq", args_dict.keys())
        child_order_execution_freq = args_dict["child_order_execution_freq"]
        _LOG.debug("Using child_order_execution_freq from Broker only args")
    _LOG.info("bar_duration=%s", bar_duration)
    _LOG.info("universe_version=%s", universe_version)
    # Get a random `test_asset_id` from the universe.
    vendor = "CCXT"
    mode = "trade"
    asset_ids = sorted(
        ivcu.get_vendor_universe_as_asset_ids(universe_version, vendor, mode)
    )
    test_asset_id = asset_ids[0]
    bid_ask = (
        "amp/oms/notebooks/Master_bid_ask_execution_analysis.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_bid_ask_execution_analysis_configs("{system_log_dir}", "{bar_duration}", "{bid_ask_data_source}", test_asset_id={test_asset_id})',
    )
    master_exec = (
        "amp/oms/notebooks/Master_execution_analysis.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_execution_analysis_configs_Cmtask4881("{system_log_dir}", \
            "{bar_duration}", \
            "{universe_version}", \
            "{child_order_execution_freq}", \
            test_asset_id={test_asset_id})',
    )
    broker_debug = (
        "amp/oms/notebooks/Master_broker_debugging.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_broker_debugging_configs_Cmtask4881("{system_log_dir}")',
    )
    portfolio_recon = (
        "amp/oms/notebooks/Master_broker_portfolio_reconciliation.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_broker_portfolio_reconciliation_configs_Cmtask5690("{system_log_dir}")',
    )
    # create list of notebooks to run.
    notebooks = [bid_ask, master_exec, broker_debug]
    if is_full_system_run:
        notebooks.append(portfolio_recon)
        publish_system_reconciliation_notebook(system_log_dir, base_dst_dir)
    for notebook, config in notebooks:
        _run_notebook(config, base_dst_dir, notebook)
