# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import collections
import logging
import os
import pathlib

import numpy as np
import pandas as pd

import core.config as cconfig
import core.dataflow as cdataf
import core.finance as cfina
import core.dataflow_model.model_evaluator as cmodel
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.utils as cdmu
import core.plotting as plot
import core.signal_processing as csigna
import dataflow_lemonade.futures_returns.pipeline as dlfrp
import helpers.dbg as dbg
import helpers.env as henv
import helpers.io_ as hio
import helpers.pickle_ as hpickl
import helpers.printing as hprint

from typing import Any, Dict, Iterable, Optional

from tqdm.auto import tqdm

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", henv.get_system_signature()[0])

# hprint.config_notebook()

# %%
def get_config_diffs(
    config_dict: collections.OrderedDict, tag_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Create a dataframe of config diffs.

    :param config_dict: dictionary of configs
    :param tag_col: name of the tag col. If tags are the same for all configs
        and `tag_col` is not None, add tags to config diffs dataframe
    :return: config diffs dataframe
    """
    diffs = cconfig.diff_configs(config_dict.values())
    non_empty_diffs = [diff for diff in diffs if len(diff) > 1]
    if non_empty_diffs:
        config_diffs = cconfig.convert_to_dataframe(diffs).dropna(
            how="all", axis=1
        )
    else:
        config_diffs = pd.DataFrame(index=range(len(diffs)))
    # If tags are the same, still add them to `config_diffs`.
    if tag_col is not None and tag_col not in config_diffs.columns:
        tags = [config[tag_col] for config in config_dict.values()]
        config_diffs[tag_col] = tags
    return config_diffs

# %%
notebook_config = cconfig.get_config_from_nested_dict({
    "exp_dir": "/app/experiment1",
    "model_evaluator_kwargs": {
        "returns_col": "ret_0_vol_adj_2",
        "predictions_col": "ret_0_vol_adj_2_hat",
        "target_volatility": 0.1,
        "oos_start": "2017-01-01",
    },
})

# %% [markdown]
# # Load results

# %%
rbs_dicts = cdmu.load_experiment_artifacts(
    notebook_config["exp_dir"],
    "result_bundle.pkl"
)

# %%
display(rbs_dicts[0]["result_df"].head(3))
display(rbs_dicts[0]["result_df"].tail(3))

# %%
evaluator = cmodel.build_model_evaluator_from_result_bundle_dicts(
    rbs_dicts,
    **notebook_config["model_evaluator_kwargs"].to_dict(),
)

# %%
evaluator.calculate_stats()

# %%
evaluator.aggregate_models(mode="ins", target_volatility=0.05)[2].to_frame()

# %%
evaluator.aggregate_models(mode="oos")[2].to_frame()

# %%
evaluator.aggregate_models(mode="all_available")[2].to_frame()

# %%
plotter = modplot.ModelPlotter(evaluator)

# %%
plotter.plot_performance(resample_rule="B")

# %%
plotter.plot_performance(
    mode="ins",
    target_volatility=0.05,
    resample_rule="B",
    plot_cumulative_returns_kwargs={"mode": "log"},
)

# %%
plotter.plot_sharpe_ratio_panel(frequencies=["B", "W", "M"])

# %%
plotter.plot_performance(
    mode="all_available",
    target_volatility=0.05,
    resample_rule="B",
    plot_cumulative_returns_kwargs={"mode": "log"},
)

# %%
