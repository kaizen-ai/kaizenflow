"""
Entry point for `run_config_list.py`.

Import as:

import dataflow.backtest.master_backtest as dtfbamaexp
"""

import logging
import os

import core.config as cconfig
import dataflow.backtest.dataflow_backtest_utils as dtfbaexuti
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hparquet as hparque

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
    #dag_runner = system_config["dag_runner_builder"]()
    fit_result_bundle = dag_runner.fit()
    # Maybe run OOS.
    if (
        "run_oos" in system_config["experiment_config"].to_dict().keys()
        and system_config["experiment_config"]
    ):
        result_bundle = dag_runner.predict()
    else:
        result_bundle = fit_result_bundle
    # Save results.
    # TODO(gp): We could return a `ResultBundle` and have
    #  `run_config_stub.py` save it.
    dtfbaexuti.save_experiment_result_bundle(system_config, result_bundle)


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
        dtfbaexuti.save_experiment_result_bundle(
            config,
            fit_rb,
            file_name="fit_result_bundle_" + training_datetime_str + ".pkl",
        )
        pred_rb.payload = payload
        dtfbaexuti.save_experiment_result_bundle(
            config,
            pred_rb,
            file_name="predict_result_bundle_" + training_datetime_str + ".pkl",
        )


# #############################################################################


def _save_tiled_output(
    system_config: cconfig.Config, result_bundle: dtfcore.ResultBundle
) -> None:
    """
    Serialize the results of a tiled experiment.

    :param result_bundle: DAG results to save
    """
    # Extract the part of the simulation for this tile (i.e., [start_timestamp,
    # end_timestamp]) discarding the warm up period (i.e., the data in
    # [start_timestamp_with_lookback, start_timestamp]).
    result_df = result_bundle.result_df.loc[
        system_config["experiment_config", "start_timestamp"] : system_config[
            "experiment_config", "end_timestamp"
        ]
    ]
    # Convert the result into Parquet.
    df = result_df.stack()
    asset_id_col_name = system_config["market_data_config", "asset_id_col_name"]
    df.index.names = ["end_ts", asset_id_col_name]
    df = df.reset_index(level=1)
    df["year"] = df.index.year
    df["month"] = df.index.month
    # The results are saved in the subdir `tiled_results` of the experiment list.
    tiled_dst_dir = os.path.join(
        system_config["experiment_config", "dst_dir"], "tiled_results"
    )
    hparque.to_partitioned_parquet(
        df, [asset_id_col_name, "year", "month"], dst_dir=tiled_dst_dir
    )
    _LOG.info("Tiled results written in '%s'", tiled_dst_dir)


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
    hdbg.dassert_isinstance(system_config_list, dtfsys.SystemConfigList)
    _LOG.debug("system_config_list=\n%s", system_config_list)
    system = system_config_list.system
    system_config = system_config_list.get_only_config()
    system.set_config(system_config)
    dag_runner = system.dag_runner
    hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
    # TODO(gp): Even this should go in the DAG creation in the builder.
    dag_runner.set_fit_intervals(
        [
            (
                system_config[
                    "experiment_config", "start_timestamp_with_lookback"
                ],
                system_config["experiment_config", "end_timestamp"],
            )
        ],
    )
    fit_result_bundle = dag_runner.fit()
    # Save results.
    _save_tiled_output(system_config, fit_result_bundle)
