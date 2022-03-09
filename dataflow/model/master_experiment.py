"""
Entry point for `run_experiment.py`.

Import as:

import dataflow.model.master_experiment as dtfmomaexp
"""

import logging
import os

import core.config as cconfig
import dataflow.core as cdataf
import dataflow.model.experiment_utils as dtfmoexuti
import helpers.hparquet as hparque

_LOG = logging.getLogger(__name__)


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
    #dag_config = config.pop("DAG")
    #dag_runner = cdataf.PredictionDagRunner(
    #    dag_config, config["meta"]["dag_builder"]
    #)
    dag_runner = config["meta", "dag_runner"](config)
    # TODO(gp): Maybe save the drawing to file?
    # cdataf.draw(dag_runner.dag)
    # TODO(gp): Why passing function instead of the values directly?
    # if "set_fit_intervals" in config["meta"].to_dict():
    #     dag_runner.set_fit_intervals(
    #         **config["meta", "set_fit_intervals", "func_kwargs"].to_dict()
    #     )
    # if "set_predict_intervals" in config["meta"].to_dict():
    #     dag_runner.set_predict_intervals(
    #         **config["meta", "set_predict_intervals", "func_kwargs"].to_dict()
    #     )
    fit_result_bundle = dag_runner.fit()
    # Maybe run OOS.
    if "run_oos" in config["meta"].to_dict().keys() and config["meta"]:
        result_bundle = dag_runner.predict()
    else:
        result_bundle = fit_result_bundle
    # Save results.
    # TODO(gp): We could return a `ResultBundle` and have
    #  `run_experiment_stub.py` save it.
    dtfmoexuti.save_experiment_result_bundle(config, result_bundle)


# #############################################################################


def run_rolling_experiment(config: cconfig.Config) -> None:
    _LOG.debug("config=\n%s", config)
    dag_config = config.pop("DAG")
    dag_runner = cdataf.RollingFitPredictDagRunner(
        dag_config,
        config["meta"]["dag_builder"],
        config["meta"]["start"],
        config["meta"]["end"],
        config["meta"]["retraining_freq"],
        config["meta"]["retraining_lookback"],
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
def _save_tiled_output(config, result_bundle):
    result_df = result_bundle.result_df.loc[
        config["meta", "start_timestamp"] : config["meta", "end_timestamp"]
    ]
    df = result_df.stack()
    asset_id_col_name = config["meta", "asset_id_col_name"]
    df.index.names = ["end_ts", asset_id_col_name]
    df = df.reset_index(level=1)
    df["year"] = df.index.year
    df["month"] = df.index.month
    # The results are saved in the subdir `tiled_results` of the experiment list.
    tiled_dst_dir = os.path.join(config["meta", "dst_dir"], "tiled_results")
    hparque.to_partitioned_parquet(
        df, [asset_id_col_name, "year", "month"], dst_dir=tiled_dst_dir
    )
    _LOG.info("Tiled results written in '%s'", tiled_dst_dir)


def run_tiled_experiment(config: cconfig.Config) -> None:
    """
    Run an experiment by:

    - creating a DAG from the passed config
    - running the DAG
    - saving the result as

    All parameters are passed through a `Config`.
    """
    _LOG.debug("config=\n%s", config)
    dag_runner = config["meta", "dag_runner"](config)
    # TODO(gp): Even this should go in the DAG creation in the builder.
    dag_runner.set_fit_intervals(
        [
            (
                config["meta", "start_timestamp_with_lookback"],
                config["meta", "end_timestamp"],
            )
        ],
    )
    fit_result_bundle = dag_runner.fit()
    _save_tiled_output(config, fit_result_bundle)
