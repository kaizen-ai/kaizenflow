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
# # Description

# %% [markdown]
# The goal of this notebook is to demonstrate the various approaches of working with `dataflow`.

# %% [markdown]
# # Imports

# %%
import logging
import os

import pandas as pd

import core.config.config_ as cconconf
import core.finance as cofinanc
import core.finance.resampling as cfinresa
import core.finance.returns as cfinretu
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_gallery_dataflow_example_config() -> cconconf.Config:
    """
    Get config, that specifies params for getting raw data.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(config["load"]["aws_profile"])
    s3_path = os.path.join(s3_bucket_path, "historical")
    config["load"]["data_dir"] = os.path.join(
        s3_path, "historical"
    )
    config["load"]["data_snapshot"] = "latest"
    config["load"]["partition_mode"] = "by_year_month"
    config["load"]["dataset"] = "ohlcv"
    config["load"]["contract_type"] = "spot"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["start_date"] = pd.Timestamp("2021-09-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2021-09-15", tz="UTC")
    config["data"]["resampling_rule"] = "5T"
    return config


# %%
config = get_gallery_dataflow_example_config()
print(config)

# %% [markdown]
# # Load historical data

# %%
# Specify params.
universe_version = "v3"
resample_1min = True
root_dir = config["load"]["data_dir"]
partition_mode = config["load"]["partition_mode"]
dataset = config["load"]["dataset"]
contract_type = config["load"]["contract_type"]
data_snapshot = config["load"]["data_snapshot"]
aws_profile = config["load"]["aws_profile"]

# Initiate the client.
historical_client = icdcl.CcxtHistoricalPqByTileClient(
    universe_version,
    resample_1min,
    root_dir,
    partition_mode,
    dataset,
    contract_type,
    data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# ### Data Loader

# %%
# Specify time period.
full_symbols = ["binance::ADA_USDT", "binance::AVAX_USDT"]
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]

# Load the data.
data_hist = historical_client.read_data(full_symbols, start_date, end_date)
display(data_hist.shape)
display(data_hist.head(3))

# %% [markdown]
# # Task description

# %% [markdown]
# The goal of this exercise is to implement the following transformations to the historical data:
# - resampling
# - VWAP, TWAP computation
# - Calculation of returns
#
# While using the different approaches to working with `dataflow` methods.
# The main feature that these methods are trying to overcome is the fact that when the raw data consists of two and more `full_symbols`, then one needs to be careful to apply transformations that needs to be implemented specifically to each `full_symbol`.
#
# These three approaches are:
# - 1) Use the "low level" functions and do loops
# - 2) Use pandas Multi-index
# - 3) Use Dataflow nodes
#
# The general rule is to use the third and second approach when possible, while keeping the first approach as a bacjup.

# %%
# The resampling frequency is the same for all approaches.
resampling_freq = config["data"]["resampling_rule"]


# %% [markdown]
# # Approach 1 - Use the "low level" functions and do loops

# %% [markdown]
# This approach does both resampling and computation of metrics and applied them individually to each `full_symbol` using the loop.

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


# %% run_control={"marked": false}
df_approach_1 = resample_calculate_twap_vwap_and_returns(
    data_hist, resampling_freq
)
df_approach_1.head(3)

# %% [markdown]
# # Approach 2 - Use pandas Multi-index

# %% run_control={"marked": false}
# Convert historical data to multiindex format.
converted_data = dtfsys._convert_to_multiindex(data_hist, "full_symbol")
converted_data.head(3)

# %%
# Resampling VWAP (besides potential errors). This implies hardcoded formula in a mix with resampling functions.
vwap_approach_2 = (converted_data["close"] * converted_data["volume"]).resample(
    resampling_freq
).mean() / converted_data["volume"].resample(resampling_freq).sum()
vwap_approach_2.head(3)

# %%
# Compute the ret_0 on all assets. You don't need a loop! But the data needs to be in the "right" format
# (the variable one wants to loop on needs to be the outermost in the levels, so one needs to do swaplevel).
rets_approach_2 = converted_data.swaplevel(axis=1).pct_change()
rets_approach_2.head(3)

# %% run_control={"marked": false}
# To go back to a flat index representation.
rets_approach_2.columns = ["_".join(col) for col in rets_approach_2.columns]
rets_approach_2.head(3)

# %% [markdown]
# # Approach 3 - Use Dataflow nodes

# %% [markdown]
# One node does resampling and VWAP, TWAP calculations, the other does returns.

# %%
# Configure the node to do the TWAP / VWAP resampling.
node_resampling_config = {
    "in_col_groups": [
        ("close",),
        ("volume",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
        "rule": resampling_freq,
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
# Put the data in the DataFlow format (which is multi-index).
converted_data = dtfsys._convert_to_multiindex(data_hist, "full_symbol")
# Create the node.
nid = "resample"
node = dtfcore.GroupedColDfToDfTransformer(
    nid,
    transformer_func=cofinanc.resample_bars,
    **node_resampling_config,
)
# Compute the node on the data.
vwap_twap = node.fit(converted_data)
# Save the result.
vwap_twap_approach_3 = vwap_twap["df_out"]
vwap_twap_approach_3.head(3)

# %%
# Configure the node to calculate the returns.
node_returns_config = {
    "in_col_groups": [
        ("close",),
        ("vwap",),
        ("twap",),
    ],
    "out_col_group": (),
    "transformer_kwargs": {
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
    nid,
    transformer_func=cofinanc.compute_ret_0,
    **node_returns_config,
)
# Compute the node on the data.
rets = node.fit(vwap_twap_approach_3)
# Save the result.
vwap_twap_rets_approach_3 = rets["df_out"]
vwap_twap_rets_approach_3.head(3)