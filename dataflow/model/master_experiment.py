"""
Entry point for `run_experiment.py`.

Import as:

import dataflow.model.master_experiment as dtfmomaexp
"""

# TODO(gp): master_run_backtest.py

import logging
import os

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model.experiment_utils as dtfmoexuti
import helpers.hdbg as hdbg
import helpers.hdatetime as hdatetime
import helpers.hparquet as hparque

_LOG = logging.getLogger(__name__)


# TODO(gp): -> run_ins_oos_backtest
def run_experiment(config: cconfig.Config) -> None:
    """
    Implement an experiment to:

    - create a DAG from the passed config
    - run the DAG
    - save the generated `ResultBundle`

    All parameters are passed through a `Config`.
    """
    _LOG.debug("config=\n%s", config)
    config = config.copy()
    dag_runner = config["dag_runner_builder"](config)
    fit_result_bundle = dag_runner.fit()
    # Maybe run OOS.
    if "run_oos" in config["experiment_config"].to_dict().keys() and config["experiment_config"]:
        result_bundle = dag_runner.predict()
    else:
        result_bundle = fit_result_bundle
    # Save results.
    # TODO(gp): We could return a `ResultBundle` and have
    #  `run_experiment_stub.py` save it.
    dtfmoexuti.save_experiment_result_bundle(config, result_bundle)


# #############################################################################


# TODO(gp): -> run_rolling_backtest
def run_rolling_experiment(config: cconfig.Config) -> None:
    _LOG.debug("config=\n%s", config)
    dag_config = config.pop("dag_config")
    dag_runner = dtfcore.RollingFitPredictDagRunner(
        dag_config,
        config["dag_builder"],
        config["experiment_config"]["start"],
        config["experiment_config"]["end"],
        config["experiment_config"]["retraining_freq"],
        config["experiment_config"]["retraining_lookback"],
    )
    for training_datetime_str, fit_rb, pred_rb in dag_runner.fit_predict():
        payload = cconfig.get_config_from_nested_dict({"config": config})
        fit_rb.payload = payload
        dtfmoexuti.save_experiment_result_bundle(
            config,
            fit_rb,
            file_name="fit_result_bundle_" + training_datetime_str + ".pkl",
        )
        pred_rb.payload = payload
        dtfmoexuti.save_experiment_result_bundle(
            config,
            pred_rb,
            file_name="predict_result_bundle_" + training_datetime_str + ".pkl",
        )


# #############################################################################


# TODO(gp): move to experiment_utils.py?
def _save_tiled_output(
    config: cconfig.Config, result_bundle: dtfcore.ResultBundle
) -> None:
    """
    Serialize the results of a tiled experiment.

    :param result_bundle: DAG results to save
    """
    start_timestamp = config["experiment_config", "start_timestamp"]
    end_timestamp = config["experiment_config", "end_timestamp"]
    # Sanity check for the tile borders.
    hdbg.dassert_lte(start_timestamp, end_timestamp)
    hdatetime.dassert_has_tz(start_timestamp)
    hdatetime.dassert_has_tz(end_timestamp)
    hdbg.dassert_eq(start_timestamp.tzinfo, end_timestamp.tzinfo,
                    "start_timestamp=%s end_timestamp=%s",
                    start_timestamp, end_timestamp)
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
    result_df = result_bundle.result_df.loc[start_timestamp:end_timestamp]
    result_df.index = result_df.index.tz_convert(start_timestamp.tzinfo)
    # Convert the result into Parquet.
    df = result_df.stack()
    asset_id_col_name = config["market_data_config", "asset_id_col_name"]
    df.index.names = ["end_ts", asset_id_col_name]
    df = df.reset_index(level=1)
    df["year"] = df.index.year
    df["month"] = df.index.month
    # The results are saved in the subdir `tiled_results` of the experiment list.
    tiled_dst_dir = os.path.join(config["experiment_config", "dst_dir"], "tiled_results")
    hparque.to_partitioned_parquet(
        df, [asset_id_col_name, "year", "month"], dst_dir=tiled_dst_dir
    )
    _LOG.info("Tiled results written in '%s'", tiled_dst_dir)


def run_tiled_backtest(config: cconfig.Config) -> None:
    """
    Run a backtest by:

    - creating a DAG from the passed config
    - running the DAG
    - saving the result as

    All parameters are passed through a `Config`.
    """
    _LOG.debug("config=\n%s", config)
    # Create the DAG runner.
    dag_runner = config["dag_runner_builder"]()
    hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
    # TODO(gp): Even this should go in the DAG creation in the builder.
    dag_runner.set_fit_intervals(
        [
            (
                config["experiment_config", "start_timestamp_with_lookback"],
                config["experiment_config", "end_timestamp"],
            )
        ],
    )
    fit_result_bundle = dag_runner.fit()
    # Save results.
    _save_tiled_output(config, fit_result_bundle)
