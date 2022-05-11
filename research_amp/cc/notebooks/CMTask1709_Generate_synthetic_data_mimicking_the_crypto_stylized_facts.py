# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# - Compute returns from the real data.
# - Pre-define the hit rate and calculate predictions, hits and confidence intervals.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
        
import logging
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import scipy.stats as stats
import seaborn as sns

import core.finance.tradability as cfintrad
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.universe as ivcu
import core.finance as cofinanc
import core.config.config_ as cconconf
import core.finance.resampling as cfinresa

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_synthetic_data_config() -> cconconf.Config:
    """
    Get config that specifies params for analysis.
    """
    config = cconconf.Config()
    # Data parameters.
    config.add_subconfig("data")
    # Reference price to calculate returns.
    config["data"]["reference_price"] = "close"
    # Returns mode: 'pct_change','log_rets' or 'diff'.
    config["data"]["rets_mode"] = "pct_change"
    # Check returns for an analysis: raw ('rets') or cleaned ('rets_cleaned').
    config["data"]["rets_col"] = "rets_cleaned"
    # Choose the timeframe for resampling.
    config["data"]["resampling_rule"] = "5T"
    return config


# %%
config = get_synthetic_data_config()
print(config)


# %% [markdown]
# # Functions

# %%
def compute_and_clean_returns(df, price_col, rets_mode, lookback, plot_rets: bool):
    # Compute returns.
    df["rets"] = cofinanc.compute_ret_0(df[price_col], rets_mode)
    # Clean them with # Rolling SMA for returns.
    df["rets_cleaned"] = df["rets"]
    df["rets_cleaned"] /= df["rets_cleaned"].rolling(lookback).std()
    if plot_rets:
        df["rets_cleaned"].plot()
    return df


# %% [markdown]
# # Extract returns from the real data

# %% [markdown]
# ## Load BTC data from `crypto-chassis`

# %%
btc_df = pd.read_csv("BTC_one_year.csv", index_col="timestamp")
ohlcv_cols = [
    "open",
    "high",
    "low",
    "close",
    "volume",
]
btc_df.index = pd.to_datetime(btc_df.index)
btc_df = btc_df[ohlcv_cols]
btc_df.head(3)

# %% [markdown]
# ## Process returns

# %%
btc = btc_df.copy()
# Specify params.
price_col = config["data"]["reference_price"]
rets_mode = config["data"]["rets_mode"]
lookback = 100
resampling_rule = config["data"]["resampling_rule"]
# Resample.
btc = cfinresa.resample_ohlcv_bars(btc, resampling_rule)
# Add returns.
btc = compute_and_clean_returns(btc, 
                          price_col, 
                          rets_mode, 
                          lookback, 
                          plot_rets=True)
# Show snippet.
display(btc.head())

# %%
# Show the distribution of returns.
rets_col = config["data"]["rets_col"]
sns.displot(btc, x=rets_col)

# %% [markdown]
# # Pre-defined Predictions, Hit Rates and Confidence Interval

# %%
sample = btc
ret_col = "rets_cleaned"
hit_rate = 0.55
seed = 2
alpha = 0.05
method = "normal"

# TODO(max): return a series of preds and then concat to the df (keep all the stuff in one DB)
hit_df = cfintrad.get_predictions_and_hits(sample, ret_col, hit_rate, seed)
display(hit_df.head(3))
cfintrad.calculate_confidence_interval(hit_df["hit"], alpha, method)

# %% [markdown]
# # PnL as a function of `hit_rate`

# %% [markdown]
# ## Show PnL for the current `hit_rate`

# %%
# A model makes a prediction on the rets and then it's realized.
pnl = (hit_df["predictions"] * hit_df[ret_col]).cumsum()
pnl.plot()

#daily_pnl = pnl.resample("1B").sum()

print((daily_pnl.mean() / daily_pnl.std()) * np.sqrt(252))

# %% [markdown]
# ## Relationship between hit rate and pnl (bootstrapping to compute pnl = f(hit_rate))

# %%
sample = btc
rets_col = config["data"]["rets_col"]
hit_rates = np.linspace(0.4, 0.6, num=10)
n_experiment = 10

pnls = cfintrad.simulate_pnls_for_set_of_hit_rates(
    sample, rets_col, hit_rates, n_experiment
)

# %% run_control={"marked": false}
hit_pnl_df = pd.DataFrame(pnls.items(), columns=["hit_rate", "PnL"])
sns.scatterplot(data=hit_pnl_df, x="hit_rate", y="PnL")

# %%
x = hit_pnl_df["hit_rate"]
y = hit_pnl_df["PnL"]

ols_results = stats.linregress(x, y)
print(f"R-squared = {ols_results.rvalue**2:.4f}")
plt.plot(x, y, 'o', label='original data')
plt.plot(x, ols_results.intercept + ols_results.slope*x, 'r', label='fitted line')
plt.legend()
plt.show()
