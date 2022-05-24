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
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import pandas as pd

import core.config.config_ as cconconf
import core.explore as coexplor
import core.signal_processing.incremental_pca as csprinpc
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import research_amp.transform as ramptran

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_CrossSectionalLearning_config() -> cconconf.Config:
    """
    Get config, that specifies params for getting raw data from `crypto
    chassis`.
    """
    config = cconconf.Config()
    # Load parameters.
    # config.add_subconfig("load")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["full_symbols"] = [
        "binance::ADA_USDT",
        "binance::BNB_USDT",
        "binance::BTC_USDT",
        "binance::DOGE_USDT",
        "binance::EOS_USDT",
        "binance::ETH_USDT",
        "binance::SOL_USDT",
        "binance::XRP_USDT",
        "binance::LUNA_USDT",
        "binance::DOT_USDT",
        "binance::LTC_USDT",
        "binance::UNI_USDT",
    ]
    config["data"]["start_date"] = pd.Timestamp("2022-01-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2022-02-01", tz="UTC")
    # Transformation parameters.
    config.add_subconfig("transform")
    config["transform"]["resampling_rule"] = "1T"
    config["transform"]["rets_type"] = "pct_change"
    # Analysis parameters.
    config.add_subconfig("analysis")
    config["analysis"][
        "reference_rets"
    ] = "close.ret_0"  # e.g., "vwap.ret_0", "twap.ret_0"
    return config


# %%
config = get_CrossSectionalLearning_config()
print(config)

# %% [markdown]
# # Load the data

# %% [markdown]
# Specs for the current data snapshot:
# - Data type: `OHLCV`
# - Universe: `v5` (excl. missing coins, see `research_amp/cc/notebooks/master_tradability_analysis.ipynb` for reference)
# - Data range: January 2022

# %%
# TODO(Max): Refactor the loading part once #1766 is implemented.

# Read from crypto_chassis directly.
# full_symbols = config["data"]["full_symbols"]
# start_date = config["data"]["start_date"]
# end_date = config["data"]["end_date"]
# ohlcv_cc = raccchap.read_crypto_chassis_ohlcv(full_symbols, start_date, end_date)

# Read saved 1 month of data.
ohlcv_cc = pd.read_csv("/shared_data/ohlcv_cc_v5.csv", index_col="timestamp")
ohlcv_cc.index = pd.to_datetime(ohlcv_cc.index)
ohlcv_cols = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "full_symbol",
]
ohlcv_cc = ohlcv_cc[ohlcv_cols]
ohlcv_cc.head(3)

# %% [markdown]
# # Compute returns

# %%
# VWAP, TWAP transformation.
resampling_rule = config["transform"]["resampling_rule"]
df = ramptran.calculate_vwap_twap(ohlcv_cc, resampling_rule)
# Returns calculation.
rets_type = config["transform"]["rets_type"]
df = ramptran.calculate_returns(df, rets_type)
# Choose reference returns to proceed to further analysis.
df = df[[config["analysis"]["reference_rets"]]]
df.head(3)

# %% [markdown]
# # Residualize returns

# %% [markdown]
# ## Estimate PCA

# %% [markdown]
# ### Rolling PCA

# %%
# Params.
sample = df["close.ret_0"].head(1000)
nan_mode = "drop"
com = 1
# Rolling PCA calculations.
corr_df, eigval_df, eigvec_df = coexplor.rolling_pca_over_time(
    sample, com, nan_mode
)
eigval_df.columns = sample.columns
eigvec_df.columns = sample.columns
coexplor.plot_pca_over_time(eigval_df, eigvec_df)

# %% [markdown]
# ### Incremental PCA

# %%
# Incremental PCA calculations.
num_pc = 2
tau = 1
lambda_df, unit_eigenvec_dfs = csprinpc.compute_ipca(sample, num_pc, tau)
unit_eigenvec_dfs[0]["binance::ADA_USDT"].plot()

# %% run_control={"marked": false}
lambda_show = lambda_df.reset_index(drop=True)
# Clean outliers manually.
lambda_show = lambda_show[lambda_show[0] < 0.000025]
lambda_show = lambda_show[lambda_show[1] < 0.0000025]
lambda_show.plot.scatter(0, 1)
