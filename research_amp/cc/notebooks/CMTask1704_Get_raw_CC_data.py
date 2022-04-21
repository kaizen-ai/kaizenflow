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

# %% [markdown]
# # Imports

# %%
import logging
import os

import numpy as np
import pandas as pd

import core.config.config_ as cconconf
import core.finance.resampling as cfinresa
import core.finance.returns as cfinretu
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_cmtask1704_config_ccxt() -> cconconf.Config:
    """
    Get config, that specifies params for getting raw data.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    env_file = imvimlita.get_db_env_path("dev")
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    config["load"]["connection"] = hsql.get_connection(*connection_params)
    config["load"]["aws_profile"] = "ck"
    config["load"]["data_dir_hist"] = os.path.join(
        "s3://cryptokaizen-data", "historical"
    )
    config["load"]["data_snapshot"] = "latest"
    config["load"]["partition_mode"] = "by_year_month"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    config["data"]["start_date"] = pd.Timestamp("2022-04-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2022-04-15", tz="UTC")
    return config


# %%
config = get_cmtask1704_config_ccxt()
print(config)

# %% [markdown]
# # Load the data

# %% [markdown]
# ## Real-time

# %%
# Specify params.
vendor = config["data"]["vendor"]
resample_1min = True
connection = config["load"]["connection"]
# Initiate the client.
ccxt_rt_client = icdcl.CcxtCddDbClient(vendor, resample_1min, connection)

# %% [markdown]
# ### Universe

# %%
# Specify the universe.
rt_universe = ccxt_rt_client.get_universe()
len(rt_universe)

# %%
# Choose cc for analysis.
full_symbols = rt_universe[0:2]
full_symbols

# %% [markdown]
# ### Data Loader

# %%
# Specify time period.
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]

# Load the data.
data = ccxt_rt_client.read_data(full_symbols, start_date, end_date)
display(data.shape)
display(data.head(3))

# %% [markdown]
# ## Historical

# %%
# Specify params.
resample_1min = True
root_dir = config["load"]["data_dir_hist"]
partition_mode = config["load"]["partition_mode"]
data_snapshot = config["load"]["data_snapshot"]
aws_profile = config["load"]["aws_profile"]

# Initiate the client.
historical_client = icdcl.CcxtHistoricalPqByTileClient(
    resample_1min,
    root_dir,
    partition_mode,
    data_snapshot=data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# ### Universe

# %%
# Specify the universe.
historical_universe = historical_client.get_universe()
len(historical_universe)

# %%
# Choose cc for analysis.
full_symbols = historical_universe[0:2]
full_symbols

# %% [markdown]
# ### Data Loader

# %%
# Specify time period.
start_date = pd.Timestamp("2021-09-01", tz="UTC")
end_date = pd.Timestamp("2021-09-15", tz="UTC")

# Load the data.
data_hist = historical_client.read_data(full_symbols, start_date, end_date)
display(data_hist.shape)
display(data_hist.head(3))

# %% [markdown]
# # Resample and calculate TWAP, VWAP

# %% [markdown]
# Here, I want to propose two versions of calculating VWAP and TWAP using the method that is already exists - `compute_twap_vwap()`.
#
# Note: I am aware of some already existing resampling functions, however, due to the format of the data, they are not completely relevant for the mixed data where groupby methods are needed.
#
# However, this method returns only VWAP, TWAP columns, so we also need to attach OHLCV data to them as well as adjust timestamps to the closed bars.
#
# Due to the complexity of calculations, I came up with two options:
# 1) First, calculate VWAP, TWAP, then resample and attach OHLCV data
#
# 2) First, resample OHLCV data, then calculate and attach VWAP, TWAP

# %%
# For this draft let's put the resampling frequency equals to 5 mins (of course, it can be customized to any value)
resampling_freq = 5


# %% [markdown]
# ## Version 1

# %% [markdown]
# - Resample and attach OHLCV data
# - Calculate vwap, twap on the basis of previously resampled data

# %%
def resampling_func_v1(df, freq):
    """
    Group by `full_symbols` and resample to the desired timing.

    :param df: Initial OHLCV data for cc
    :param freq: Desired resampling frequency (in minutes)
    :return: Grouped and resampled cc OHLCV data
    """
    # Shift timestamps to indicate the end of the bar in the initial data.
    df = df.shift(1, freq="T")
    # Construct the resampling frequency.
    resampling_freq = f"{freq}min"
    # Create a resampler that takes data for each `full_symbol` and resample it to the given time.
    resampler = df.reset_index().groupby(
        [
            "full_symbol",
            pd.Grouper(key="timestamp", freq=resampling_freq, closed="right"),
        ]
    )
    # Organize OHLCV values according to the resampler.
    new_df = resampler.agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    # Shift timestamps to indicate the end of the bar in the resampled data.
    new_df = new_df.reset_index("full_symbol").shift(freq, freq="T")
    return new_df


# %%
def calculate_twap_vwap_v1(cc_df, res_rule):
    """
    :param cc_df: Grouped and resampled cc OHLCV data form `resampling_func()`
    :param res_rule: Desired resampling frequency (same as in `resampling_func()`)
    :return: Resampled data with TWAP and VWAP values
    """
    # Supported variables.
    result = []
    full_symbols = cc_df["full_symbol"].unique()
    # Calculate TWAP and VWAP for each `full_symbol`
    for cc in full_symbols:
        df = cc_df[cc_df["full_symbol"] == cc]
        df = pd.concat(
            [
                df,
                cfinresa.compute_twap_vwap(
                    df, f"{res_rule}T", price_col="close", volume_col="volume"
                ),
            ],
            axis=1,
        )
        result.append(df)
    # Collect the results into a single dataframe.
    twap_vwap_df = pd.concat(result)
    return twap_vwap_df


# %%
resampled_v1 = resampling_func_v1(data, resampling_freq)
resampled_v1.head(3)

# %%
twap_vwap_v1 = calculate_twap_vwap_v1(resampled_v1, resampling_freq)
twap_vwap_v1.head(3)


# %% [markdown]
# ## Version 2

# %% [markdown]
# - Calculate vwap, twap on the basis of the initial data
# - Resample and attach OHLCV data

# %%
def calculate_twap_vwap_v2(cc_df, res_rule):
    """
    :param cc_df: Initial OHLCV data
    :param res_rule: Desired resampling frequency
    :return: Resampled timestamps with TWAP and VWAP values
    """
    # Supported variables.
    result = []
    full_symbols = cc_df["full_symbol"].unique()
    # Calculate TWAP and VWAP for each `full_symbol`
    for cc in full_symbols:
        df = cc_df[cc_df["full_symbol"] == cc]
        twap_vwap_values = cfinresa.compute_twap_vwap(
            df, f"{res_rule}T", price_col="close", volume_col="volume"
        )
        # Move timestamp to the end of the bar.
        cc_wp = twap_vwap_values.shift(res_rule, freq="T")
        # Add `full_symbol` column.
        cc_wp["full_symbol"] = cc
        result.append(cc_wp)
    # Collect the results into a single dataframe.
    twap_vwap_df = pd.concat(result)
    return twap_vwap_df


# %%
def resampling_func_v2(df, freq):
    """
    Group by `full_symbols` and resample to the desired timing.

    :param df: Initial OHLCV data for cc
    :param freq: Desired resampling frequency (in minutes)
    :return: Grouped and resampled cc OHLCV data
    """
    # Shift timestamps to indicate the end of the bar in the initial data.
    df = df.shift(1, freq="T")
    # Construct the resampling frequency.
    resampling_freq = f"{freq}min"
    # Create a resampler that takes data for each `full_symbol` and resample it to the given time.
    resampler = df.reset_index().groupby(
        [
            "full_symbol",
            pd.Grouper(key="timestamp", freq=resampling_freq, closed="right"),
        ]
    )
    # Organize OHLCV values according to the resampler.
    new_df = resampler.agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    # Shift timestamps to indicate the end of the bar in the resampled data.
    new_df = new_df.reset_index("full_symbol").shift(freq, freq="T")
    # Cosmetic improvements.
    new_df = new_df.set_index("full_symbol", append=True)
    new_df = new_df.reorder_levels(order=["full_symbol", "timestamp"])
    return new_df


# %%
twap_vwap_v2 = calculate_twap_vwap_v2(data, resampling_freq)
twap_vwap_v2.head(3)

# %%
resampled_v2 = resampling_func_v2(data, resampling_freq)
resampled_v2.head(3)

# %%
# Some cosmetic improvements to vwap, twap df.
indexed_twap_vwap_v2 = twap_vwap_v2.set_index(
    "full_symbol", append=True
).reorder_levels(order=["full_symbol", "timestamp"])
# Combine vwap, twap and ohlcv data.
twap_vwap_v2 = pd.concat([resampled_v2, indexed_twap_vwap_v2], axis=1)
twap_vwap_v2.head(3)


# %% [markdown]
# # Calculate returns

# %%
def calculate_returns(df):
    grouper = df.groupby(["full_symbol"])
    df["vwap_ret"] = (df["vwap"] / grouper["vwap"].shift(1)) - 1
    df["twap_ret"] = (df["twap"] / grouper["twap"].shift(1)) - 1
    df["log_ret"] = np.log(df["close"]) - np.log(grouper["close"].shift(1))
    return df


# %%
rets_df = calculate_returns(twap_vwap_v2)
rets_df

# %%
# Some stats and visualizations.
ada_ex = rets_df.loc[["binance::ADA_USDT"]].reset_index("full_symbol")[
    ["log_ret", "vwap_ret", "twap_ret"]
]
display(ada_ex.corr())
ada_ex.plot()


# %% [markdown]
# ## New proposal

# %%
def resample_calculate_twap_vwap_and_returns(df, resampling_freq):
    result = []
    full_symbol_list = df["full_symbol"].unique()
    for cc in full_symbol_list:
        # DataFrame with a specific `full_symbol`
        cc_df = df[df["full_symbol"] == cc]
        # Resample OHLCV data inside `full_symbol`-specific DataFrame.
        resampled_cc_df = cfinresa.resample_ohlcv_bars(
            cc_df, rule=resampling_freq
        )
        # Attach VWAP, TWAP.
        resampled_cc_df[["vwap", "twap"]] = cfinresa.compute_twap_vwap(
            cc_df, resampling_freq, price_col="close", volume_col="volume"
        )
        # Calculate returns.
        resampled_cc_df["vwap_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["vwap"]], "pct_change"
        )
        resampled_cc_df["twap_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["twap"]], "pct_change"
        )
        resampled_cc_df["log_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["close"]], "log_rets"
        )
        # Add a column with `full_symbol` indication.
        resampled_cc_df["full_symbol"] = cc
        # Omit unnecesary columns.
        resampled_cc_df = resampled_cc_df.drop(columns=["open", "high", "low"])
        result.append(resampled_cc_df)
    final_df = pd.concat(result)
    return final_df


# %%
df = resample_calculate_twap_vwap_and_returns(data_hist, "5T")
df.head(3)

# %%
# The only missing part here is to place timestamps at the end of the bar.
# Right now I have only this hardcoded solution:
new_df = df.shift(5, freq="T")

# %% run_control={"marked": false}
# Stats and vizualisation to check the outcomes.
ada_ex = new_df[new_df["full_symbol"] == "binance::ADA_USDT"][
    ["log_rets", "vwap_rets", "twap_rets"]
]
display(ada_ex.corr())
ada_ex.plot()

# %%

# %%

# %%
import dataflow.system.source_nodes as dtfsysonod

# %%
data = data_hist

# %%
mm = dtfsysonod._convert_to_multiindex(data, "full_symbol")
mm

# %%
mm.info()

# %%
cfinresa.resample(
            mm, rule="5T"
        )#.agg({"close":"last"})

# %%
cfinresa.resample_ohlcv_bars(
            mm, rule="5T"
        )


# %% [markdown]
# 4 level of compatibility
# - reinvent the wheel(s)
#     - This is not ok!
# 1) use the "low level" functions and do loops
# 2) use pandas Multi-index
# 3) use Dataflow nodes
#
#
# TODO(Max): Extract from this notebook a Gallery_dataflow_example notebook that reads the historical data, computes the same stuff in the 3 ways

# %%
# Approach 1) does both resampling and ret.
def resample_calculate_twap_vwap_and_returns(df, resampling_freq):
    result = []
    full_symbol_list = df["full_symbol"].unique()
    for cc in full_symbol_list:
        # DataFrame with a specific `full_symbol`
        cc_df = df[df["full_symbol"] == cc]
        # Resample OHLCV data inside `full_symbol`-specific DataFrame.
        resampled_cc_df = cfinresa.resample_ohlcv_bars(
            cc_df, rule=resampling_freq
        )
        # Attach VWAP, TWAP.
        resampled_cc_df[["vwap", "twap"]] = cfinresa.compute_twap_vwap(
            cc_df, resampling_freq, price_col="close", volume_col="volume"
        )
        # Calculate returns.
        resampled_cc_df["vwap_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["vwap"]], "pct_change"
        )
        resampled_cc_df["twap_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["twap"]], "pct_change"
        )
        resampled_cc_df["log_rets"] = cfinretu.compute_ret_0(
            resampled_cc_df[["close"]], "log_rets"
        )
        # Add a column with `full_symbol` indication.
        resampled_cc_df["full_symbol"] = cc
        # Omit unnecesary columns.
        resampled_cc_df = resampled_cc_df.drop(columns=["open", "high", "low"])
        result.append(resampled_cc_df)
    final_df = pd.concat(result)
    return final_df


# %%
dd = mm.copy()
dd.columns = ['_'.join(col) for col in dd.columns]

# %%
mm.info()

# %%
dd.head(3)

# %%

# %%
cfinresa.compute_twap_vwap(
            dd, "5T", price_col="close_binance::ADA_USDT", volume_col="volume_binance::AVAX_USDT"
        )

# %%

# %% [markdown]
# ## Approach 2): multi-index

# %%
# TWAP.
mm.resample("5T").mean()

# %%
# Approach 2) resampling VWAP (besides potential errors)
(mm["close"] * mm["volume"]).resample("5T").mean() / mm["volume"].resample("5T").sum()

# %%
# Approach 2) multi-index.

# Conceptually the operation above is the same.
# Compute the ret_0 on all assets. You don't need a loop! But the data needs to be in the "right" format
# (the variable you want to loop on needs to be the outermost in the levels, so you do swaplevel)
df2.swaplevel(axis=1).pct_change()

# %% [markdown]
# ## Approach 3) data-flow style

# %%
# Approach 3): one node does resampling, the other does return.
import dataflow.core as dtfcore
import core.finance as cofinanc

# Put the data in the DataFlow format (which is multi-index).
mm = dtfsysonod._convert_to_multiindex(data, "full_symbol")

# Configure the node to do the TWAP / VWAP resampling.
node_config = {
        "in_col_groups": [
            ("close",),
            ("volume",),
            #("day_spread",),
            #("day_num_spread",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "rule": "5T",
            "resampling_groups": [
                ({"close": "close"}, "last", {}),
                (
                    {
                        "close": "twap",
                    },
                    "mean",
                    {},
                ),
                (
                    {
                        #"day_spread": "day_spread",
                        #"day_num_spread": "day_num_spread",
                        "volume": "volume",
                    },
                    "sum",
                    {"min_count": 1},
                ),
            ],
            "vwap_groups": [
                ("close", "volume", "vwap"),
            ],
        },
        "reindex_like_input": False,
        "join_output_with_input": False,
    }

# Create the node.
nid = "resample"
node = dtfcore.GroupedColDfToDfTransformer(
    nid, transformer_func=cofinanc.resample_bars, **node_config,
)

# Compute the node on the data.
df = node.fit(mm)

# %%
# Show the result.
df2 = df["df_out"]
df2

# %%
# Compute returns on the resampled data DataFlow-style.

# Those are the parameters to pass to the node.
node_config = {
    "in_col_groups": [
        ("close",),
        ("vwap",),
        ("twap",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
        #"mode": "log_rets",
        "mode": "pct_change",
    },
    "col_mapping": {
        "close": "close.ret_0",
        "vwap": "vwap.ret_0",
        "twap": "twap.ret_0",
    },
}

# Create the node that computes ret_0.
nid = "ret0"
node = dtfcore.GroupedColDfToDfTransformer(
    nid, transformer_func=cofinanc.compute_ret_0, **node_config,
)

df3 = node.fit(df2)

# %%
df3["df_out"]
