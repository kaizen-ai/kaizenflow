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

import dev_scripts.lib_tasks_run_notebooks as dsltruno
"""

import logging
import os

from invoke import task

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _run_notebook(
    config_builder: str, base_dst_dir: str, notebook_path: str
) -> None:
    """
    Run analysis notebooks and store it in a specified location.

    :param base_dst_dir: top most directory to store data into
    :param notebook_path: relative path to the notebook to execute, assuming amp is a submodule.
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
    hsystem.system(
        script_name, suppress_output=False, log_level="echo", abort_on_error=False
    )
    # Move the ipynb.html file to s3.
    if hs3.is_s3_path(base_dst_dir):
        html_bucket_path = henv.execute_repo_config_code("get_html_bucket_path()")
        notebook_name = os.path.basename(notebook_path)
        notebook_name = os.path.splitext(notebook_name)[0]
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
    else:
        hio.create_dir(base_dst_dir, incremental=True)
        hdbg.dassert_dir_exists(base_dst_dir)
        _LOG.info("Copying results from '%s' to '%s'", results_dir, base_dst_dir)
        cmd = f"cp -vr {results_dir} {base_dst_dir}"
        hsystem.system(script_name, suppress_output=False, log_level="echo")
    # Delete the temp dir
    hio.delete_dir(results_dir)


@task
def run_notebooks(
    ctx, system_log_dir, base_dst_dir, system_log_dir_recon=None
):  # type: ignore
    """
    Run cross dataset reconciliation notebook and store it in a specified
    location.

    :param system_log_dir: dir to store logs
    :param base_dst_dir: dir to store data reconciliation
    :param system_log_dir_recon: input log dir for Master_system_reconciliation_fast.ipynb
    """
    _ = ctx
    bid_ask = (
        "amp/oms/notebooks/Master_bid_ask_execution_analysis.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_bid_ask_execution_analysis_configs_Cmtask4881("{system_log_dir}")',
    )
    master_exec = (
        "amp/oms/notebooks/Master_execution_analysis.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_execution_analysis_configs_Cmtask4881("{system_log_dir}")',
    )
    broker_debug = (
        "amp/oms/notebooks/Master_broker_debugging.ipynb",
        "amp.oms.execution_analysis_configs."
        + f'get_broker_debugging_configs_Cmtask4881("{system_log_dir}")',
    )
    # create list of notebooks to run.
    notebooks = [bid_ask, master_exec, broker_debug]
    if system_log_dir_recon:
        master_sys_recon = (
            "amp/oms/notebooks/Master_system_reconciliation_fast.ipynb",
            f'amp.oms.get_reconciliation_configs("{system_log_dir_recon}")',
        )
        notebooks.append(master_sys_recon)
    for notebook, config in notebooks:
        _run_notebook(config, base_dst_dir, notebook)
