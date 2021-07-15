"""
Entry point for `run_experiment.py`.
"""

import logging

import core.config as cconfig
import core.dataflow as cdataf
import core.dataflow_model.utils as cdtfut

_LOG = logging.getLogger(__name__)


def run_experiment(config: cconfig.Config) -> None:
    """
    Implement the master experiment to:

    - create a DAG from the passed config
    - run the DAG
    - save the generated `ResultBundle`

    All parameters are passed through a `Config`.
    """
    _LOG.debug("config=\n%s", config)
    dag_config = config.pop("DAG")
    dag_runner = cdataf.PredictionDagRunner(
        dag_config, config["meta"]["dag_builder"]
    )
    # TODO(gp): Maybe save the drawing to file?
    # cdataf.draw(dag_runner.dag)
    # TODO(gp): Why passing function instead of the values directly?
    if "set_fit_intervals" in config["meta"].to_dict():
        dag_runner.set_fit_intervals(
            **config["meta", "set_fit_intervals", "func_kwargs"].to_dict()
        )
    if "set_predict_intervals" in config["meta"].to_dict():
        dag_runner.set_predict_intervals(
            **config["meta", "set_predict_intervals", "func_kwargs"].to_dict()
        )
    fit_result_bundle = dag_runner.fit()
    # Process payload.
    payload = cconfig.get_config_from_nested_dict({"config": config})
    if "run_oos" in config["meta"].to_dict().keys() and config["meta"]:
        result_bundle = dag_runner.predict()
        payload["fit_result_bundle"] = fit_result_bundle.to_config()
    else:
        result_bundle = fit_result_bundle
    result_bundle.payload = payload
    # Save results.
    # TODO(gp): We could return a `ResultBundle` and have
    # `run_experiment_stub.py` save it.
    cdtfut.save_experiment_result_bundle(config, result_bundle)


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
        cdtfut.save_experiment_result_bundle(
            config,
            fit_rb,
            file_name="fit_result_bundle_" + training_datetime_str + ".pkl",
        )
        pred_rb.payload = payload
        cdtfut.save_experiment_result_bundle(
            config,
            pred_rb,
            file_name="predict_result_bundle_" + training_datetime_str + ".pkl",
        )
