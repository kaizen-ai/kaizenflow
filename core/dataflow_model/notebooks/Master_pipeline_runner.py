# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import core.config_builders as ccbuild
import core.dataflow as cdataf
import helpers.dbg as dbg
import helpers.env as henv
import helpers.pickle_ as hpickl
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
config = ccbuild.get_config_from_env()

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
payload = ccbuild.get_config_from_nested_dict({"config": config})

# %%
if "run_oos" in config["meta"].to_dict().keys() and config["meta"]:
    result_bundle = dag_runner.predict()
    payload["fit_result_bundle"] = fit_result_bundle.to_config()
else:
    result_bundle = fit_result_bundle

# %%
result_bundle.payload = payload

# %%
try:
    path = os.path.join(
        config["meta", "experiment_result_dir"], "result_bundle.pkl"
    )
    if True:
        hpickl.to_pickle(result_bundle.to_config().to_dict(), path)
except AssertionError:
    _LOG.warning("Unable to serialize results.")
