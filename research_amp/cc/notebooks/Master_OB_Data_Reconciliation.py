# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
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

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_example_config() -> cconfig.Config:
    """
    Config for comparison of 1sec CryptoChassis and 1sec CCXT bid/ask data
    """
    # TODO(Danya): parameters: 1sec/1min, bid/ask//ohclv, vendors
    config = cconfig.Config()
    param_dict = {
        "data": {
            # Whether to resample 1sec data to 1min using our production flow.
            "resample_1min" = False
            # Parameters for client initialization.
            "cc_im_client": {
                "universe_version": None,
                "resample_1min": False,
                "contract_type": "futures",
                "tag":"downloaded_1sec"
            },
            "ccxt_im_client": {
                "resample_1min": False,
                "db_connection": hsql.get_connection(
                    *hsql.get_connection_info_from_env_file(
                        imvimlita.get_db_env_path("dev")
                    )
                ),
                "table_name": "ccxt_bid_ask_futures_raw",
            },
            # Parameters for data query.
            "read_data": {
                # Get start/end ts as inputs to script.
                "start_ts": pd.Timestamp("2022-11-15 00:00:00+00:00"),
                "end_ts": pd.Timestamp("2022-11-15 00:00:15+00:00"),
                "columns": None,
                "filter_data_mode": "assert",
            },
        },
        "column_names": {
            "bid_ask_cols": [
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
            ],
        },
        "order_level": 1,
    }
    config = cconfig.Config.from_dict(param_dict)
    return config


config = get_example_config()
print(config)

# %% [markdown]
# # Clients

# %%
# CCXT client.
ccxt_im_client = icdcl.CcxtSqlRealTimeImClient(**config["data"]["ccxt_im_client"])
# CC client.
cc_parquet_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(**config["data"]["cc_im_client"])

# %% [markdown]
# # Universe

# %%
# DB universe
ccxt_universe = ccxt_im_client.get_universe()
# CC universe.
cc_universe = cc_parquet_client.get_universe()
# Intersection of universes that will be used for analysis.
universe = list(set(ccxt_universe) & set(cc_universe))

# %%
compare_universe = hprint.set_diff_to_str(
    cc_universe, ccxt_universe, add_space=True
)
print(compare_universe)

# %% [markdown]
# # Load data

# %% [markdown]
# ## Load CCXT

# %% run_control={"marked": true}
ccxt_df = ccxt_im_client.read_data(universe, **config["data"]["read_data"])

# %%
ccxt_df.head(10)

# %% [markdown]
# On the first glance:
# - It has levels where they are not expected to be
# - The level columns are empty

# %% [markdown]
# ### Clean CCXT data

# %% run_control={"marked": true}
# Remove level suffix in the TOB column name.
ccxt_df.columns = ccxt_df.columns.str.replace("_1", "")
# Remove all levels.
target_columns = [col for col in ccxt_df.columns if not col[-1].isnumeric()]
target_columns = [col for col in target_columns if col!="end_download_timestamp"]
ccxt_df = ccxt_df[target_columns]
# CCXT timestamp data goes up to milliseconds, so one needs to round it to seconds.
ccxt_df.index = ccxt_df.reset_index()["timestamp"].apply(
            lambda x: x.round(freq="S")
        )
ccxt_df = ccxt_df.reset_index().set_index(["timestamp", "full_symbol"])
display(ccxt_df.head(10))

# %% [markdown]
# ## Load CCXT

# %%
cc_df = cc_parquet_client.read_data(universe, **config["data"]["read_data"])
cc_df = cc_df.reset_index().set_index(["timestamp", "full_symbol"])
display(cc_df.head(10))

# %% [markdown]
# # Resampling data

# %%
resample_1min = config.get_and_mark_as_used(("data", "resample_1min"))
if resample_1min:
    ccxt_df = 

# %% [markdown]
# # Analysis

# %% [markdown]
# ## Merge CC and DB data into one DataFrame
#

# %%
data = ccxt_df.merge(
    cc_df,
    how="outer",
    left_index=True,
    right_index=True,
    suffixes=("_ccxt", "_cc"),
)
_LOG.info("Start date = %s", data.reset_index()["timestamp"].min())
_LOG.info("End date = %s", data.reset_index()["timestamp"].max())
_LOG.info(
    "Avg observations per coin = %s",
    len(data) / len(data.reset_index()["full_symbol"].unique()),
)
# Move the same metrics from two vendors together.
data = data.reindex(sorted(data.columns), axis=1)
# NaNs observation.
_LOG.info(
    "Number of observations with NaNs in CryptoChassis = %s",
    len(data[data["bid_price_cc"].isna()]),
)
_LOG.info(
    "Number of observations with NaNs in CCXT = %s",
    len(data[data["bid_price_ccxt"].isna()]),
)
# Remove NaNs.
data = hpandas.dropna(data, report_stats=True)
#
display(data.tail())

# %% [markdown]
# ## Calculate differences

# %%
# Full symbol will not be relevant in calculation loops below.
bid_ask_cols = config["column_names"]["bid_ask_cols"]
# Each bid ask value will have a notional and a relative difference between two sources.
for col in bid_ask_cols:
    # Notional difference: CC value - DB value.
    data[f"{col}_diff"] = data[f"{col}_cc"] - data[f"{col}_ccxt"]
    # Relative value: (CC value - DB value)/DB value.
    data[f"{col}_relative_diff_pct"] = (
        100 * (data[f"{col}_cc"] - data[f"{col}_ccxt"]) / data[f"{col}_ccxt"]
    )

# %%
# Calculate the mean value of differences for each coin.
diff_stats = []
grouper = data.groupby(["full_symbol"])
for col in bid_ask_cols:
    diff_stats.append(grouper[f"{col}_diff"].mean())
    diff_stats.append(grouper[f"{col}_relative_diff_pct"].mean())
#
diff_stats = pd.concat(diff_stats, axis=1)

# %% [markdown]
# ## Show stats for differences (in %)

# %% [markdown]
# ### Prices

# %%
display(diff_stats[["bid_price_relative_diff_pct", "ask_price_relative_diff_pct"]])

# %% [markdown]
# ### Sizes

# %%
display(diff_stats[["bid_size_relative_diff_pct", "ask_size_relative_diff_pct"]])

# %% [markdown]
# ## Correlations

# %% [markdown]
# ### Bid price

# %%
bid_price_corr_matrix = (
    data[["bid_price_cc", "bid_price_ccxt"]].groupby(level=1).corr()
)
display(bid_price_corr_matrix)

# %% [markdown]
# ### Ask price

# %%
ask_price_corr_matrix = (
    data[["ask_price_cc", "ask_price_ccxt"]].groupby(level=1).corr()
)
display(ask_price_corr_matrix)

# %% [markdown]
# ### Bid size

# %%
bid_size_corr_matrix = (
    data[["bid_size_cc", "bid_size_ccxt"]].groupby(level=1).corr()
)
display(bid_size_corr_matrix)

# %% [markdown]
# ### Ask size

# %%
ask_size_corr_matrix = (
    data[["ask_size_cc", "ask_size_ccxt"]].groupby(level=1).corr()
)
display(ask_size_corr_matrix)
