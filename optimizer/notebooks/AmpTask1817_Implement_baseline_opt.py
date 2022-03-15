# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

# %%
import logging

import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import optimizer.single_period_optimization as osipeopt

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

# _LOG.info("%s", henv.get_system_signature()[0])

# %%
hprint.config_notebook()

# %% [markdown]
# # Build forecast dataframe

# %%
idx = [1, 2, 3]

# %%
sigma = pd.Series(index=idx, data=[0.05, 0.07, 0.08], name="volatility")
holdings = pd.Series(index=idx, data=[1000, 1500, -500], name="position")
predictions = pd.Series(index=idx, data=[0.05, 0.09, 0.03], name="prediction")

# %%
df = pd.concat([holdings, predictions, sigma], axis=1)

# %%
df.index.name = "asset_id"

# %%
df

# %%
df = df.reset_index()

# %%
df

# %% [markdown]
# # Build optimizer config

# %%
dict_ = {
    "volatility_penalty": 0.75,
    "dollar_neutrality_penalty": 0.1,
    "turnover_penalty": 0.0,
    "target_gmv": 3000,
    "target_gmv_upper_bound_multiple": 1.01,
}

spo_config = cconfig.get_config_from_nested_dict(dict_)

# %% [markdown]
# # Optimize

# %%
spo = osipeopt.SinglePeriodOptimizer(spo_config, df)

# %%
opt_results = spo.optimize()
display(opt_results.round(3))

# %%
spo.compute_stats(opt_results)

# %%
assert 0

# %%
df = pd.DataFrame(
    [
        [0.000843, -0.001103, 0.001103, 24000],
        [-0.000177, -0.000344, 0.000344, -17000],
        [-0.000238, -0.000130, 0.000130, -90000],
    ],
    [101, 201, 301],
    ["prediction", "returns", "volatility", "position"],
)
df.index.name = "asset_id"
df = df.reset_index()