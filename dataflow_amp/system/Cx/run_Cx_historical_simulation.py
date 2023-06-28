#!/usr/bin/env python
"""
The script performs several actions:

    - runs historical simulation for all configs
    - runs historical simulation for 1 config with specified index and extended logs

The following command runs rolling simulation for all configs:
    ```
    > dataflow_amp/system/Cx/run_Cx_historical_simulation.py \
        --action run_all_configs \
        --dag_builder_ctor_as_str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
        --backtest_config ccxt_v7_1-all.5T.2022-06-01_2022-12-01 \
        --train_test_mode rolling \
        --fit_at_beginning True \
    ```

The following command runs rolling simulation for the 3rd configs:
    ```
    > dataflow_amp/system/Cx/run_Cx_historical_simulation.py \
        --action run_single_config \
        --dag_builder_ctor_as_str "dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder" \
        --backtest_config ccxt_v7_1-all.5T.2022-06-01_2022-12-01 \
        --train_test_mode rolling \
        --fit_at_beginning True \
        --config_idx 3
    ```
"""
# TODO(Grisha): maybe it is worth it to remove the wrapper and call
# directly `amp/dataflow/backtest/run_config_list.py` everywhere.
import argparse
import logging
import os
import re

import dataflow.system as dtfsys
import dataflow_amp.system.Cx.Cx_forecast_system_example as dtfasccfsex
import dataflow_amp.system.Cx.Cx_tile_config_builders as dtfascctcbu
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hparser as hparser
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def _get_DagBuilder_name(dag_builder_ctor_as_str: str) -> str:
    """
    Get `DagBuilder` name from DagBuilder ctor passed as a string.
    """
    # E.g., `dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder` ->
    # `[dataflow_orange, pipelines, C1, C1b_pipeline, C1b_DagBuilder]`.
    dag_builder_split = dag_builder_ctor_as_str.split(".")
    hdbg.dassert_lt(0, len(dag_builder_split))
    dag_builder_name = dag_builder_split[-1]
    re_pattern = r"\w+(?=(_DagBuilder))"
    dag_builder_name_match = re.match(re_pattern, dag_builder_name)
    hdbg.dassert(
        dag_builder_name_match,
        msg=f"Make sure that `DagBuilder` is in the name={dag_builder_name}",
    )
    dag_builder_name = dag_builder_name_match[0]
    return dag_builder_name


def get_config_builder(
    dag_builder_ctor_as_str: str,
    fit_at_beginning: bool,
    train_test_mode: str,
    backtest_config: str,
    *,
    oos_start_date_as_str: str = None,
) -> dtfsys.SystemConfigList:
    """
    Get a config builder function to pass it to the executable.

    :param dag_builder_ctor_as_str: same as in `Cx_NonTime_ForecastSystem`
    :param fit_at_beginning: force the system to fit before making predictions
    :param train_test_mode: same as in `Cx_NonTime_ForecastSystem`
    :param backtest_config: see `apply_backtest_config()`
    :param oos_start_date_as_str: used only for train_test_mode="ins_oos",
        see `dtfasccfsex.apply_ins_oos_backtest_config()`
    """
    system = dtfasccfsex.get_Cx_NonTime_ForecastSystem_for_simulation_example1(
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode=train_test_mode,
        backtest_config=backtest_config,
        oos_start_date_as_str=oos_start_date_as_str,
    )
    config_builder = dtfascctcbu.build_tile_config_list(system, train_test_mode)
    return config_builder


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--action",
        action="store",
        required=True,
        type=str,
        choices=["run_all_configs", "run_single_config"],
        help="Whether to run all configs or just 1 config with detailed logs",
    )
    parser.add_argument(
        "--dag_builder_ctor_as_str",
        action="store",
        required=True,
        type=str,
        help="a `DagBuilder` constructor, e.g., `dataflow_orange.pipelines.C1.C1b_pipeline.C1b_DagBuilder`",
    )
    parser.add_argument(
        "--backtest_config",
        action="store",
        required=True,
        type=str,
        help="Backtest config, e.g., 'ccxt_v7-all.5T.2021-08-01_2022-07-01'",
    )
    parser.add_argument(
        "--train_test_mode",
        action="store",
        required=True,
        type=str,
        help="""
        How to perform the prediction:
            - "ins": fit and predict using the same data
            - "ins_oos": fit using a train dataset and predict using a test dataset
            - "rolling": run a DAG by periodic fitting on previous history and evaluating on new data
        """,
    )
    parser.add_argument(
        "--fit_at_beginning",
        action="store",
        required=False,
        type=bool,
        default=None,
        help="""
        - Force the system to fit before making predictions
        - Applicable only for ML models
        """,
    )
    parser.add_argument(
        "--oos_start_date_as_str",
        action="store",
        required=False,
        type=str,
        help="Out-of-sample start date date for `train_test_mode` = 'ins_oos'",
    )
    parser.add_argument(
        "--config_idx",
        action="store",
        required=False,
        type=int,
        default=0,
        help="Index of the config generated by `config_builder` to run",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print the command and exit without running it",
    )
    parser.add_argument(
        "--tag",
        action="store",
        help="Destination dir tag, e.g., `run0`",
    )
    parser = hparser.add_verbosity_arg(parser)
    # TODO(gp): For some reason, not even this makes mypy happy.
    # cast(argparse.ArgumentParser, parser)
    return parser  # type: ignore


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # TODO(Grisha): consider enabling running from amp.
    hdbg.dassert(
        hgit.is_amp_present(),
        msg="The script is runnable only from a repo that contains amp but not from amp itself.",
    )
    action = args.action
    dag_builder_ctor_as_str = args.dag_builder_ctor_as_str
    backtest_config = args.backtest_config
    train_test_mode = args.train_test_mode
    fit_at_beginning = args.fit_at_beginning
    #
    if train_test_mode == "ins":
        experiment_builder = (
            "amp.dataflow.backtest.master_backtest.run_in_sample_tiled_backtest"
        )
    elif train_test_mode == "ins_oos":
        experiment_builder = (
            "amp.dataflow.backtest.master_backtest.run_ins_oos_tiled_backtest"
        )
    elif train_test_mode == "rolling":
        experiment_builder = (
            "amp.dataflow.backtest.master_backtest.run_rolling_tiled_backtest"
        )
    else:
        raise ValueError(f"Unsupported train_test_mode={train_test_mode}")
    #
    if train_test_mode == "ins_oos":
        oos_start_date_as_str = args.oos_start_date_as_str
        config_builder = f'amp.dataflow_amp.system.Cx.run_Cx_historical_simulation.get_config_builder("{dag_builder_ctor_as_str}",{fit_at_beginning},train_test_mode="{train_test_mode}",backtest_config="{backtest_config}",oos_start_date_as_str="{oos_start_date_as_str}")'
    else:
        config_builder = f'amp.dataflow_amp.system.Cx.run_Cx_historical_simulation.get_config_builder("{dag_builder_ctor_as_str}",{fit_at_beginning},train_test_mode="{train_test_mode}",backtest_config="{backtest_config}")'
    tag = args.tag
    dag_builder_name = _get_DagBuilder_name(dag_builder_ctor_as_str)
    dst_dir = f"build_tile_configs.{dag_builder_name}.{backtest_config}.{train_test_mode}.{tag}"
    # Set major opts.
    opts = [
        f"--experiment_builder {experiment_builder}",
        f"--config_builder '{config_builder}'",
        f"--dst_dir {dst_dir}",
    ]
    opts = " ".join(opts)
    # Set script name and additional opts based on the action.
    if action == "run_all_configs":
        script_name = "amp/dataflow/backtest/run_config_list.py"
        opts += " --clean_dst_dir --no_confirm --num_threads serial"
        log_file_name = "run_config_list.txt"
    elif action == "run_single_config":
        script_name = "amp/dataflow/backtest/run_config_stub.py"
        config_idx = args.config_idx
        opts += f" --config_idx {config_idx}"
        log_file_name = "log.txt"
    else:
        raise ValueError(f"Unsupported action={action}")
    opts += f" -v DEBUG 2>&1 | tee {log_file_name}"
    cmd = f"{script_name} {opts}"
    if args.dry_run:
        _LOG.warning("Dry-run: cmd='%s'", cmd)
    else:
        # TODO(Grisha): this is a work-around so that one does not overwrite a
        # dir by accident, to solve properly "Use the run_config_list script to
        # run historical simulations" CmTask #4609.
        if os.path.exists(dst_dir):
            _LOG.warning("Dir '%s' already exists", dst_dir)
            hsystem.query_yes_no(
                f"Do you want to delete the dir '{dst_dir}'",
                abort_on_no=True,
            )
        hsystem.system(cmd, suppress_output=False, log_level="echo")
        # TODO(Grisha): this is a work-around so that one does not overwrite a
        # log file by accident, to solve properly "Use the run_config_list script to
        # run historical simulations" CmTask #4609.
        # Copy a log file to `dst_dir` so that one does not overwrite it by accident.
        log_file_path = os.path.join(dst_dir, log_file_name)
        mv_cmd = f"mv {log_file_name} {log_file_path}"
        hsystem.system(mv_cmd)


if __name__ == "__main__":
    _main(_parse())
