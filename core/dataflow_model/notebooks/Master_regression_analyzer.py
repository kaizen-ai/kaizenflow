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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import numpy as np
import pandas as pd
import seaborn as sns

import core.config as cconfig
import core.dataflow_model.incremental_single_name_model_evaluator as ime
import core.dataflow_model.model_evaluator as modeval
import core.dataflow_model.model_plotter as modplot
import core.dataflow_model.regression_analyzer as cdmra
import core.dataflow_model.stats_computer as csc
import core.dataflow_model.utils as cdmu
import core.plotting as cplot
import core.statistics as cstati
import helpers.dbg as dbg
import helpers.printing as hprint

# %%
dbg.init_logger(verbosity=logging.INFO)
# dbg.init_logger(verbosity=logging.DEBUG)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", env.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load regression dataframes

# %%
src_dir = ""
file_name = "result_bundle.v2_0.pkl"

fit_iter = cdmu.yield_experiment_artifacts(
    src_dir=src_dir,
    file_name=file_name,
    load_rb_kwargs={},
)

# %%
fit_coeffs = {k: v.info["ml"]["predict"]["fit_coefficients"] for k, v in fit_iter}
fit_coeffs = pd.concat(fit_coeffs)

# %%
fit_coeffs.head()

# %% [markdown]
# # MHT

# %%
p_vals = fit_coeffs["p_val_2s"]

# %%
p_vals.hist(bins=30)

# %%
cdmra.compute_moments(fit_coeffs, ["p_val_2s"])

# %%
q_vals = cstati.estimate_q_values(p_vals)

# %%
q_vals.hist(bins=30)

# %% [markdown]
# # Feature stats
#

# %%
feature_stats = cdmra.compute_moments(
    fit_coeffs, ["rho", "beta", "beta_z_scored", "turn"]
)
display(feature_stats)

# %%
sweep = cstati.apply_smoothing_parameters(
    feature_stats[("rho", "mean")],
    feature_stats[("turn", "mean")],
    np.arange(0, 3, 0.1),
)

# %%
stat = "beta"
feature = ""
fit_coeffs[stat].xs(feature, level=1).hist(bins=101)

# %% [markdown]
# # Reweight

# %%
feature_cols = []
target_col = ""

art_iter = cdmu.yield_experiment_artifacts(
    src_dir=src_dir,
    file_name=file_name,
    load_rb_kwargs={"columns": feature_cols + [target_col]},
)


def get_feature_weights(key: int) -> pd.Series:
    ...


# %%
sharpes = {}
turns = {}
daily_pnls = {}
portfolio_pnl = pd.Series()
for key, art in art_iter:
    features = art.result_df[feature_cols]
    prediction = (features * get_feature_weights(key)).sum(min_count=1, axis=1)
    turns[key] = cstati.compute_avg_turnover_and_holding_period(prediction)
    pnl = prediction * art.result_df[target_col]
    portfolio_pnl = pnl.add(portfolio_pnl, fill_value=0)
    sharpes[key] = cstati.compute_annualized_sharpe_ratio(pnl)
    daily_pnls[key] = pnl.resample("B").sum(min_count=1)

# %%
daily_pnl_xs = pd.DataFrame(daily_pnls).mean(axis=1)
daily_portfolio_pnl = portfolio_pnl.resample("B").sum(min_count=1)

# %%
daily_portfolio_pnl.cumsum().plot()

# %%

# %% [markdown]
# # Pair plots

# %%
split1 = cdmra.compute_coefficients(
    src_dir=src_dir,
    file_name=file_name,
    feature_cols=feature_cols,
    target_col=target_col,
    start=None,
    end=None,
)

split2 = cdmra.compute_coefficients(
    src_dir=src_dir,
    file_name=file_name,
    feature_cols=feature_cols,
    target_col=target_col,
    start=None,
    end=None,
)

# %%
stat = ""
sns.pairplot(
    pd.concat(
        [
            split1[stat].rename("split1"),
            split2[stat].rename("split2"),
        ],
        join="inner",
        axis=1,
    )
)

# %%
stat = ""
feature = ""
sns.pairplot(
    pd.concat(
        [
            split1[stat].xs(feature, level=1).rename("split1"),
            split2[stat].xs(feature, level=1).rename("split2"),
        ],
        join="inner",
        axis=1,
    )
)
