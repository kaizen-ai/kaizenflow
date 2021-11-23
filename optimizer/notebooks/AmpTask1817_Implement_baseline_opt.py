# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

# %%
import io

import pandas as pd

import core.config as cconfig
import optimizer.single_period_optimization as osipeopt

# %%
idx = ["asset1", "asset2", "asset3", "cash"]

# %%
spread = pd.Series(index=idx, data=[0.01, 0.01, 0.01, 0.0], name="spread")
sigma = pd.Series(index=idx, data=[0.05, 0.07, 0.08, 0.0], name="sigma")
holdings = pd.Series(index=idx, data=[1000, 1500, -500, 1000], name="holdings")
predictions = pd.Series(index=idx, data=[0.05, 0.09, 0.03, 0], name="predictions")

# %%
out_buffer = io.StringIO()

# %%
dict_ = {
    "costs": {
        "risk": {
            "class": "DiagonalRiskModel",
            "path": io.StringIO(sigma.to_csv()),
            "gamma": 1,
        },
        "dollar_neutrality": {
            "class": "DollarNeutralityCost",
            "gamma": 0.1,
        },
        "spread": {
            "class": "SpreadCost",
            "path": io.StringIO(spread.to_csv()),
            "gamma": 0.1,
        },
        "turnover": {
            "class": "TurnoverCost",
            "gamma": 0.1,
        },
    },
    "constraints": {
        "leverage": {
            "class": "LeverageConstraint",
            "leverage_bound": 2,
        },
        "concentration_bound": {
            "class": "ConcentrationConstraint",
            "concentration_bound": 0.003,
        },
        "dollar_neutrality": {
            "class": "DollarNeutralityConstraint",
        },
    },
    "holdings": io.StringIO(holdings.to_csv()),
    "predictions": io.StringIO(predictions.to_csv()),
    "result_path": out_buffer,
}

spo_config = cconfig.get_config_from_nested_dict(dict_)

# %%
osipeopt.perform_single_period_optimization(spo_config)

# %%
pd.read_csv(io.StringIO(out_buffer.getvalue()), index_col=0)

# %%
