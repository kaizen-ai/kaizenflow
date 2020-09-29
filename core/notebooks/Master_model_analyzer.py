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

# %% [markdown]
# # Description
#
# - Initialize with returns, predictions, target volatility, and oos start date
# - Evaluate portfolios generated from the predictions

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.config_builders as cfgb
import core.model_evaluator as modeval
import core.model_plotter as modplot
import core.statistics as stats
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", env.get_system_signature()[0])

prnt.config_notebook()

# %% [markdown]
# # Notebook config

# %%
eval_config = cfgb.get_config_from_nested_dict(
    {
        "target_volatility": 0.1,
        "oos_start": "2007-01-01",
        "bh_adj_threshold": 0.1,
        "resample_rule": "W",
        "mode": "ins",
    }
)

# %% [markdown]
# # Generate mock returns and predictions
#
# - This is a placeholder
# - In practice, the user should supply `rets` and `preds`

# %%
# Generate synthetic returns.
mn_process = sig_gen.MultivariateNormalProcess()
mn_process.set_cov_from_inv_wishart_draw(dim=32, seed=0)
realization = mn_process.generate_sample(
    {"start": "2000-01-01", "end": "2010-01-01", "freq": "B"}, seed=0
)
rets = realization.to_dict(orient="series")

# %%
# Generate fake predictions.
noise = sig_gen.MultivariateNormalProcess(
    pd.Series(data=[0] * 32), pd.DataFrame(np.identity(32))
)
noise_draw = noise.generate_sample(
    {"start": "2000-01-01", "end": "2010-01-01", "freq": "B"}, seed=0
)
pred_df = 0.01 * realization + 0.1 * noise_draw
# Adjust so that all models have positive SR.
pred_df = (
    stats.compute_annualized_sharpe_ratio(pred_df.multiply(realization))
    .apply(np.sign)
    .multiply(pred_df)
)

# %%
preds = pred_df.to_dict(orient="series")

# %% [markdown]
# # Initialize ModelEvaluator and ModelPlotter

# %%
evaluator = modeval.ModelEvaluator(
    returns=rets,
    predictions=preds,
    target_volatility=eval_config["target_volatility"],
    oos_start=eval_config["oos_start"],
)
plotter = modplot.ModelPlotter(evaluator)

# %%
plotter.plot_multiple_tests_adjustment(
    threshold=eval_config["bh_adj_threshold"], mode=eval_config["mode"]
)

# %%
pnl_stats = evaluator.calculate_stats(mode=eval_config["mode"])
pnl_stats

# %%
plotter.plot_correlation_matrix(
    series="pnls",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
plotter.plot_correlation_matrix(
    series="returns",
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
col_mask = pnl_stats.loc["adj_pval"] < eval_config["bh_adj_threshold"]
selected = pnl_stats.loc[:, col_mask].columns.to_list()

# %%
plotter.plot_multiple_pnls(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
evaluator.aggregate_models(
    keys=selected,
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)[2].to_frame()

# %%
plotter.plot_sharpe_ratio_panel(keys=selected, mode=eval_config["mode"])

# %%
plotter.plot_rets_signal_analysis(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_performance(
    keys=selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_rets_and_vol(
    selected,
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_positions(
    keys=selected,
    mode=eval_config["mode"],
    target_volatility=eval_config["target_volatility"],
)

# %%
plotter.plot_returns_and_predictions(
    keys=selected[:1],
    resample_rule=eval_config["resample_rule"],
    mode=eval_config["mode"],
)

# %%
