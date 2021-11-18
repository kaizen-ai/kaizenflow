# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description
#
# This notebook is the entry point for `run_notebook.py`
# It is conceptually equivalent to `core/dataflow_model/master_experiment.py` for the `run_experiment.py` flow
#
# This notebook:
# - get a config from the environment
# - create a DAG from the passed config
# - run the DAG
# - save the generated `ResultBundle`

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import core.config as cconfig
import core.dataflow as cdataf
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.pickle_ as hpickle
import helpers.printing as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
config = cconfig.get_config_from_env()

# %%
dag_config = config.pop("DAG")

# %%
dag_runner = cdataf.PredictionDagRunner(dag_config, config["meta"]["dag_builder"])

# %%
cdataf.draw(dag_runner.dag)

# %%
if "set_fit_intervals" in config["meta"].to_dict():
    dag_runner.set_fit_intervals(
        **config["meta", "set_fit_intervals", "func_kwargs"].to_dict()
    )
if "set_predict_intervals" in config["meta"].to_dict():
    dag_runner.set_predict_intervals(
        **config["meta", "set_predict_intervals", "func_kwargs"].to_dict()
    )

# %%
fit_result_bundle = dag_runner.fit()

# %%
payload = cconfig.get_config_from_nested_dict({"config": config})

# %%
if "run_oos" in config["meta"].to_dict().keys() and config["meta"]:
    result_bundle = dag_runner.predict()
    payload["fit_result_bundle"] = fit_result_bundle.to_config()
else:
    result_bundle = fit_result_bundle

# %%
result_bundle.payload = payload

# %%
# TODO(gp): Use  `cdtfut.save_experiment_result_bundle(config, result_bundle)`
try:
    path = os.path.join(
        config["meta", "experiment_result_dir"], "result_bundle.pkl"
    )
    if True:
        hpickle.to_pickle(result_bundle.to_config().to_dict(), path)
except AssertionError:
    _LOG.warning("Unable to serialize results.")
