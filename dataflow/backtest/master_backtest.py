"""
Entry point for `run_config_list.py`.

Import as:

import dataflow.backtest.master_backtest as dtfbamaexp
"""

import logging
import os
from typing import Optional

import pandas as pd

import core.config as cconfig
import dataflow.backtest.dataflow_backtest_utils as dtfbdtfbaut
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hpickle as hpickle
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# TODO(gp): -> run_ins_oos_backtest
# TODO(gp): It should accept system_config_list: cconfig.ConfigList
def run_experiment(system_config: cconfig.Config) -> None:
    """
    Implement an experiment to:

    - create a DAG from the passed config
    - run the DAG
    - save the generated `ResultBundle`

    All parameters are passed through a `Config`.
    """
    _LOG.debug("system_config=\n%s", system_config)
    system_config = system_config.copy()
    # dag_runner = system_config["dag_runner_builder"]()
    fit_result_bundle = dag_runner.fit()
    # Maybe run OOS.
    if (
        "run_oos" in system_config["backtest_config"].to_dict().keys()
        and system_config["backtest_config"]
    ):
        result_bundle = dag_runner.predict()
    else:
        result_bundle = fit_result_bundle
    # Save results.
    # TODO(gp): We could return a `ResultBundle` and have
    #  `run_config_stub.py` save it.
    dtfbdtfbaut.save_experiment_result_bundle(system_config, result_bundle)


# #############################################################################
# run_rolling_backtest
# #############################################################################


def _get_single_system_config(
    system_config_list: dtfsys.SystemConfigList,
):
    # Create the DAG runner customizing the System with the specific config.
    hdbg.dassert_isinstance(system_config_list, dtfsys.SystemConfigList)
    _LOG.debug("system_config_list=\n%s", system_config_list)
    system = system_config_list.system
    system_config = system_config_list.get_only_config()
    system.set_config(system_config)
    return system


def run_tiled_rolling_backtest(
    system_config_list: dtfsys.SystemConfigList,
) -> None:
    """
    The output looks like:

    ```
    > find build_tile_configs.E8f.eg_v2_0-top3.5T.2020-01-01_2020-03-01.run1/ | sort
    build_tile_configs.run1/
    build_tile_configs.run1/fit_results
    build_tile_configs.run1/fit_results/fit_0_20191208_000000
    build_tile_configs.run1/fit_results/fit_0_20191208_000000/fit_result_df.parquet
    build_tile_configs.run1/fit_results/fit_0_20191208_000000/fit_result_df.pq
    build_tile_configs.run1/fit_results/fit_0_20191208_000000/fit_state.json
    build_tile_configs.run1/fit_results/fit_0_20191208_000000/fit_state.pkl
    build_tile_configs.run1/fit_results/fit_1_20191215_000000
    build_tile_configs.run1/fit_results/fit_1_20191215_000000/fit_result_df.parquet
    ...
    build_tile_configs.run1/result_0
    build_tile_configs.run1/result_0/config.pkl
    build_tile_configs.run1/result_0/config.txt
    build_tile_configs.run1/result_0/run_config_list.0.log
    build_tile_configs.run1/tiled_results
    build_tile_configs.run1/tiled_results/egid=10025
    build_tile_configs.run1/tiled_results/egid=10025/year=2020
    build_tile_configs.run1/tiled_results/egid=10025/year=2020/month=1
    build_tile_configs.run1/tiled_results/egid=10025/year=2020/month=1/data.parquet
    ...
    ```
    """
    system = _get_single_system_config(system_config_list)
    #
    system.config["backtest_config", "retraining_freq"] = "1W"
    system.config["backtest_config", "retraining_lookback"] = 3
    # Build the dag runner.
    dag_runner = system.dag_runner
    hdbg.dassert_isinstance(dag_runner, dtfcore.RollingFitPredictDagRunner)
    dst_dir = os.path.join(system.config["backtest_config", "dst_dir"], "fit_results")
    # This loop corresponds to evaluating the model on a tile, but it's done in
    # chunks to do fit / predict.
    pred_rbs = []
    for idx, data in enumerate(dag_runner.fit_predict()):
        training_datetime_str, fit_rb, pred_rb = data
        _LOG.debug(hprint.to_str("idx training_datetime_str"))
        _LOG.debug("fit_rb=\n%s", str(fit_rb))
        _LOG.debug("pred_rb=\n%s", str(pred_rb))
        # After each training session, we want to save all the info from training
        # and the model stats (including weights) into an extra directory.
        # Note that the fit result_df don't form a tile (since they overlap) so we
        # just save them as they are.
        dag = dag_runner.dag
        fit_state = dtfcore.get_fit_state(dag)
        dst_dir_tmp = os.path.join(dst_dir, f"fit_{idx}_{training_datetime_str}")
        file_name = os.path.join(dst_dir_tmp, "fit_state.pkl")
        hpickle.to_pickle(
            fit_state, file_name, log_level=logging.DEBUG
        )
        file_name = os.path.join(dst_dir_tmp, "fit_state.json")
        hpickle.to_json(
            file_name, fit_state
        )
        # Save fit data.
        file_name = os.path.join(dst_dir_tmp, "fit_result_df.parquet")
        result_df = fit_rb.result_df
        hparque.to_parquet(result_df, file_name, log_level=logging.DEBUG)
        #
        file_name = os.path.join(dst_dir_tmp, "predict_result_df.parquet")
        result_df = pred_rb.result_df
        hparque.to_parquet(result_df, file_name, log_level=logging.DEBUG)
        # Concat prediction output to convert into a tile.
        pred_rbs.append(pred_rb.result_df)
    # The invariant is that concatenating all the OOS prediction you get exactly a
    # tile.
    pred_rb = pd.concat(pred_rbs, axis=0)
    # Save results.
    _save_tiled_output(system.config, pred_rb)


# #############################################################################
# run_tiled_backtest
# #############################################################################


def _save_tiled_output(
    system_config: cconfig.Config,
    result_df: pd.DataFrame,
    tag: Optional[str] = None,
) -> None:
    """
    Serialize the results of a tiled experiment.

    :param result_bundle: DAG results to save
    """
    start_timestamp = system_config["backtest_config", "start_timestamp"]
    end_timestamp = system_config["backtest_config", "end_timestamp"]
    # Sanity check for the tile borders.
    hdbg.dassert_lte(start_timestamp, end_timestamp)
    hdateti.dassert_has_tz(start_timestamp)
    hdateti.dassert_has_tz(end_timestamp)
    hdbg.dassert_eq(
        start_timestamp.tzinfo,
        end_timestamp.tzinfo,
        "start_timestamp=%s end_timestamp=%s",
        start_timestamp,
        end_timestamp,
    )
    # Extract the part of the simulation for this tile (i.e., [start_timestamp,
    # end_timestamp]) discarding the warm up period (i.e., the data in
    # [start_timestamp_with_lookback, start_timestamp]).
    # Note that we need to save the resulting data with the same timezone as the
    # tile boundaries to ensure that there is no overlap.
    # E.g., assume that the resulting data for a tile falls into
    # `[2022-06-01 00:00:00-00:00, 2022-06-30 23:55:00-00:00]`
    # If it's saved in ET timezone it's going to become
    # `[2022-05-31 20:00:00-04:00, 2022-06-30 19:55:00-04:00]` so it's going to
    # span two months potentially overwriting some other tile.
    result_df = result_df.loc[start_timestamp:end_timestamp]
    result_df.index = result_df.index.tz_convert(start_timestamp.tzinfo)
    # Convert the result into Parquet.
    df = result_df.stack()
    asset_id_col_name = system_config["market_data_config", "asset_id_col_name"]
    df.index.names = ["end_ts", asset_id_col_name]
    df = df.reset_index(level=1)
    df["year"] = df.index.year
    df["month"] = df.index.month
    # The results are saved in the subdir `tiled_results` of the experiment list.
    tiled_dst_dir = os.path.join(
        system_config["backtest_config", "dst_dir"], "tiled_results"
    )
    if tag:
        tiled_dst_dir += "." + tag
    hparque.to_partitioned_parquet(
        df, [asset_id_col_name, "year", "month"], dst_dir=tiled_dst_dir
    )
    _LOG.info("Tiled results written in '%s'", tiled_dst_dir)


# TODO(gp): This does a single fit -> run_in_sample_tiled_backtest
def run_tiled_backtest(
    system_config_list: dtfsys.SystemConfigList,
) -> None:
    """
    Run a backtest by:

    - creating a DAG from the passed config
    - running the DAG
    - saving the result as

    All parameters are passed through a `Config`.
    """
    # Create the DAG runner customizing the System with the specific config.
    system = _get_single_system_config(system_config_list)
    #
    dag_runner = system.dag_runner
    # TODO(gp): -> FitPredictDagRunner?
    hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
    # TODO(gp): Even this should go in the DAG creation in the builder.
    dag_runner.set_fit_intervals(
        [
            (
                system.config["backtest_config", "start_timestamp_with_lookback"],
                system.config["backtest_config", "end_timestamp"],
            )
        ],
    )
    fit_result_bundle = dag_runner.fit()
    # Save results.
    result_df = fit_result_bundle.result_df
    _save_tiled_output(system.config, result_df)