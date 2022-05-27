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
import os

import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import core.explore as coexplor
import core.signal_processing.incremental_pca as csprinpc
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as imvccdcccc
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
    param_dict = {
        "data": {
            # Parameters for client initialization.
            "im_client": {
                "universe_version": "v1",
                "resample_1min": True,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"),
                    "reorg",
                    "historical.manual.pq",
                ),
                "partition_mode": "by_year_month",
                "data_snapshot": "latest",
                "aws_profile": "ck",
            },
            # Parameters for data query.
            "read_data": {
                "start_ts": pd.Timestamp("2022-01-01 00:00", tz="UTC"),
                "end_ts": pd.Timestamp("2022-04-01 00:00", tz="UTC"),
                "columns": None,
                "filter_data_mode": "assert",
            },
            "transform": {
                "ohlcv_cols": [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "full_symbol",
                ],
                "resampling_rule": "5T",
                "rets_type": "pct_change",
            },
        },
        "analysis": {
            "reference_rets": "close.ret_0",  # e.g.,"vwap.ret_0", "twap.ret_0"
            "rets_type": "volume",
        },
        "model": {
            "delay_lag": 1,
            "num_lags": 4,
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


# %%
config = get_CrossSectionalLearning_config()
print(config)

# %% [markdown]
# # Load the data

# %%
# Initiate the client.
client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
    **config["data"]["im_client"]
)
# Get universe of `full_symbols`.
universe = client.get_universe()
# Load OHLCV data.
ohlcv_cc = client.read_data(universe, **config["data"]["read_data"])
# Post-processing.
ohlcv_cc = ohlcv_cc[config["data"]["transform"]["ohlcv_cols"]]
ohlcv_cc.head(3)

# %%
# Loaded universe.
print(ohlcv_cc["full_symbol"].unique())

# %% [markdown]
# # Compute returns

# %%
# VWAP, TWAP transformation.
df = ramptran.calculate_vwap_twap(
    ohlcv_cc, config["data"]["transform"]["resampling_rule"]
)
# Returns calculation.
df = ramptran.calculate_returns(df, config["data"]["transform"]["rets_type"])
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
