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
import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.utils as cdmu
import core.statistics as stats
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

# prnt.config_notebook()

# %% [markdown]
# # Notebook config

# %%
eval_config = cconfig.get_config_from_nested_dict(
    {
        # "exp_dir": "/app/experiment1",
        "model_evaluator_kwargs": {
            "returns_col": "ret_0_vol_adj_2",
            "predictions_col": "ret_0_vol_adj_2_hat",
            "oos_start": "2017-01-01",
        },
        "bh_adj_threshold": 0.1,
        "resample_rule": "W",
        "mode": "ins",
        "target_volatility": 0.1,
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
data_dict = {}
for k in pred_df.columns:
    data_dict[k] = pd.concat([rets[k].rename("returns"), pred_df[k].rename("predictions")], axis=1) 

# %% [markdown]
# # Initialize ModelEvaluator and ModelPlotter

# %%
if eval_config.get("exp_dir", None) is None:
    evaluator = modeval.ModelEvaluator2(
        data=data_dict,
        target_col="returns",
        prediction_col="predictions",
        oos_start=eval_config["model_evaluator_kwargs", "oos_start"],
    )
else:
    rbs_dicts = cdmu.load_experiment_artifacts(
        eval_config["exp_dir"],
        "result_bundle.pkl"
    )
    evaluator = modeval.build_model_evaluator_from_result_bundle_dicts(
        rbs_dicts,
        **eval_config["model_evaluator_kwargs"].to_dict(),
    )

plotter = modplot.ModelPlotter(evaluator)

# %%
plotter.plot_multiple_tests_adjustment(
    threshold=eval_config["bh_adj_threshold"], mode=eval_config["mode"]
)

# %%
pnl_stats = evaluator.calculate_stats(mode=eval_config["mode"], target_volatility=eval_config["target_volatility"])
pnl_stats.loc[["signal_quality", "correlation"]]

# %%
plotter.plot_correlation_matrix(
    series="pnl",
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
col_mask = pnl_stats.loc["signal_quality"].loc["sr.adj_pval"] < eval_config["bh_adj_threshold"]
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
