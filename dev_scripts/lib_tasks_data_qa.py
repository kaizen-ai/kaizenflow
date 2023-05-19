"""
Invokes in the file are runnable from a Docker container only.

Examples:
```
docker> invoke run_single_dataset_qa_notebook \
    --stage 'preprod' \
    --start-timestamp '2023-01-25T16:35:00+00:00' \
    --end-timestamp '2023-01-25T16:45:00+00:00' \
    --dataset-signature 'realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7.ccxt.binance.v1_0_0' \
    --aws-profile 'ck' \
    --base-dst-dir '/shared_data/ecs/preprod/data_qa/periodic_10min'

to run outside a Docker container:
```
> invoke docker_cmd --cmd 'invoke run_single_dataset_qa_notebook ...'
```
```
docker> invoke run_cross_dataset_qa_notebook \
    --stage 'preprod' \
    --start-timestamp '2023-01-26T00:00:00+00:00' \
    --end-timestamp '2023-01-26T23:59:00+00:00' \
    --dataset-signature1 'realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v7.ccxt.binance.v1_0_0' \
    --dataset-signature2 'periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0' \
    --aws-profile 'ck' \
    --base-dst-dir '/shared_data/ecs/preprod/data_qa/periodic_daily'
```

Import as:

import dev_scripts.lib_tasks_data_qa as dsltdare
"""

import logging
import os
import re
from typing import Any, Dict

from invoke import task

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hio as hio
import oms.lib_tasks_reconcile as olitarec

_LOG = logging.getLogger(__name__)


def _create_dir_for_data_qa(
    base_dst_dir: str,
    start_timestamp: str,
    end_timestamp: str,
    db_table: str,
    abort_if_exists=True,
) -> str:
    """
    Create dirs for storing data QA results.

    Final dirs layout is:
    ```
    {base_dst_dir}/
        {timestamp_dst_dir}/
            {db_table}/
                result_0/
            ...
    ```

    i.e.
    ```
    shared_data/ecs/preprod/data_qa/
         20221021_000000.20221021_210000 \
             ccxt_ohlcv_futures_preprod/
                result_0
            ...
    ```

    :param base_dst_dir: base directory (most likely shared) to store data
     QA results
    :param start_timestamp: start of the QA time interval
    :param end_timestamp: end of the QA time interval
    :param db_table: DB table the QA is done for, at the moment assuming
     there is only a single QA done on a particular table and particular
     time range.
    :param abort_if_exists: see `hio.create_dir()`
    :return: path to the created target dir
    """
    # Transform the timestamp arguments to avoid special characters
    #  i.e. 2022-11-01T00:02:00+00:00 -> 20221101_000200
    #  the context isn't lost since the raw args are present in the saved notebook
    #  and also chances of using anything else as UTC are low.
    start_timestamp = start_timestamp.replace("+00:00", "")
    end_timestamp = end_timestamp.replace("+00:00", "")
    start_timestamp = re.sub(r"[^A-Za-z0-9 ]+", "", start_timestamp)
    end_timestamp = re.sub(r"[^A-Za-z0-9 ]+", "", end_timestamp)
    start_timestamp = start_timestamp.replace("T", "_")
    end_timestamp = end_timestamp.replace("T", "_")
    timestamp_dst_dir = f"{start_timestamp}.{end_timestamp}"
    target_dir = os.path.join(base_dst_dir, timestamp_dst_dir, db_table)
    # Create a dir for QA results.
    hio.create_dir(target_dir, incremental=True, abort_if_exists=abort_if_exists)
    # Sanity check the created dirs.
    cmd = f"ls -lh {target_dir}"
    olitarec._system(cmd)
    return target_dir


def _run_data_qa_notebook(
    config_dict: Dict[str, Any], base_dst_dir: str, notebook_path: str
) -> None:
    """
    Run data QA notebook and store it in a specified location.

    The function encapsulates common behavior, concrete QA flows parametrize
    it for particular use cases.

    :param base_dst_dir: top most directory to store data QA into
    :param notebook_path: relative path to the notebook to execute, assuming amp is a submodule.
    """
    # TODO(Juraj): this does not work in the cmamp prod container when ran
    #  via AWS ECS.
    # hdbg.dassert(
    #    hserver.is_inside_docker(), "This is runnable only inside Docker."
    # )
    config = cconfig.Config.from_dict(config_dict)
    os.environ["CK_DATA_RECONCILIATION_CONFIG"] = config.to_python()
    # Set directory to store results locally
    results_dir = "."
    cmd_txt = []
    config_builder = (
        "amp.im_v2.common.data.qa.qa_check.build_dummy_data_reconciliation_config()"
    )
    opts = (
        "--num_threads 'serial' --allow_errors --publish_notebook -v DEBUG 2>&1"
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
    # Make the script executable and run it.
    _LOG.info("Running the notebook=%s", notebook_path)
    olitarec._system(script_name)
    # Assert directory generated by `run_notebook` was created.`
    results_dir = os.path.join(results_dir, "result_0")
    # hdbg.dassert_dir_exists(results_dir)
    # Copy the published notebook to the specified folder.
    # TODO(Juraj): This is a make-do solution to avoid ridiculously long
    #  directory names when running cross dataset QA. For now this should not
    #  cause conflicts.
    dir_specifier = (
        "dataset_signature" if "dataset_signature" in config_dict else "dataset_signature1"
    )
    target_dir = _create_dir_for_data_qa(
        base_dst_dir,
        config_dict["start_timestamp"],
        config_dict["end_timestamp"],
        config_dict[dir_specifier],
    )
    hdbg.dassert_dir_exists(target_dir)
    _LOG.info("Copying results from '%s' to '%s'", results_dir, target_dir)
    cmd = f"cp -vr {results_dir} {target_dir}"
    olitarec._system(cmd)
    # This is a workaround to get outcome of the data reconciliation from the notebook.
    reconc_outcome = hio.from_file("/app/ck_data_reconciliation_outcome.txt")
    if reconc_outcome.strip() == "SUCCESS":
        _LOG.info(
            "Data reconciliation was successful, results stored in '%s'",
            target_dir,
        )
    else:
        hdbg.dfatal(
            message=f"Data reconciliation failed:\n {reconc_outcome} \n"
            + f"Results stored in '{target_dir}'"
        )


@task
def run_single_dataset_qa_notebook(
    ctx,
    start_timestamp,
    end_timestamp,
    base_dst_dir,
    dataset_signature,
    stage,
    aws_profile=None,
    bid_ask_accuracy=None,
):
    """
    Run single data QA notebook and store it in a specified location.

    See `im_v2.ccxt.data.extract.compare_realtime_and_historical` for
    reconcilation params description.

    :param base_dst_dir: dir to store data reconciliation
    """
    config_dict = {
        "stage": stage,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "aws_profile": aws_profile,
        "dataset_signature": dataset_signature,
        "bid_ask_accuracy": bid_ask_accuracy,
    }
    _ = ctx
    notebook_path = "amp/im_v2/ccxt/data/qa/notebooks/data_qa_ohlcv.ipynb"
    _run_data_qa_notebook(config_dict, base_dst_dir, notebook_path)


# TODO(Juraj): temporary solution, the refactoring will be completed to use
#  dataset signatures for all datasets in #CmTask3475
@task
def run_cross_dataset_qa_notebook(
    ctx,
    start_timestamp,
    end_timestamp,
    base_dst_dir,
    dataset_signature1,
    dataset_signature2,
    stage,
    aws_profile=None,
    bid_ask_accuracy=None,
):  # type: ignore
    """
    Run cross dataset reconciliation notebook and store it in a specified
    location.

    See `im_v2.ccxt.data.extract.compare_realtime_and_historical` for
    reconcilation params description.

    :param base_dst_dir: dir to store data reconciliation
    """
    config_dict = {
        "stage": stage,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "aws_profile": aws_profile,
        "dataset_signature1": dataset_signature1,
        "dataset_signature2": dataset_signature2,
        "bid_ask_accuracy": bid_ask_accuracy,
    }
    _ = ctx
    # TODO(Juraj): come up with a more modular solution to executing the correct notebook.
    data_type = "bid_ask" if "bid_ask" in dataset_signature1 else "ohlcv"
    notebook_path = f"amp/im_v2/common/data/qa/notebooks/cross_dataset_qa_{data_type}.ipynb"
    _run_data_qa_notebook(config_dict, base_dst_dir, notebook_path)
