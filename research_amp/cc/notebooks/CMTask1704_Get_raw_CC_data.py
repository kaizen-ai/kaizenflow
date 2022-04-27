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

import pandas as pd

import core.config.config_ as cconconf
import core.finance as cofinanc
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita
import im_v2.talos.data.client.talos_clients as imvtdctacl

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
    # Data range for real-time data.
    config["data"]["start_date_rt"] = pd.Timestamp("2022-04-01", tz="UTC")
    config["data"]["end_date_rt"] = pd.Timestamp("2022-04-15", tz="UTC")
    # Data range for historical data.
    config["data"]["start_date_hist"] = pd.Timestamp("2022-01-01", tz="UTC")
    config["data"]["end_date_hist"] = pd.Timestamp("2022-01-15", tz="UTC")
    # Transformation parameters.
    config.add_subconfig("transform")
    config["transform"]["resampling_rule"] = "5T"
    config["transform"]["rets_type"] = "pct_change"
    return config


# %%
config = get_cmtask1704_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
def calculate_vwap_twap(df: pd.DataFrame, resampling_rule: str) -> pd.DataFrame:
    """
    Resample the data and calculate VWAP, TWAP using DataFlow methods.

    :param df: Raw data
    :param resampling_rule: Desired resampling frequency
    :return: Resampled multiindex DataFrame with computed metrics
    """
    # Configure the node to do the TWAP / VWAP resampling.
    node_resampling_config = {
        "in_col_groups": [
            ("close",),
            ("volume",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "rule": resampling_rule,
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
    converted_data = dtfsysonod._convert_to_multiindex(df, "full_symbol")
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
    vwap_twap_df = vwap_twap["df_out"]
    return vwap_twap_df


# %%
def calculate_returns(df: pd.DataFrame, rets_type: str) -> pd.DataFrame:
    """
    Compute returns on the resampled data DataFlow-style.

    :param df: Resampled multiindex DataFrame
    :param rets_type: i.e., "log_rets" or "pct_change"
    :return: The same DataFrame but with attached columns with returns
    """
    # Configure the node to calculate the returns.
    node_returns_config = {
        "in_col_groups": [
            ("close",),
            ("vwap",),
            ("twap",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "mode": rets_type,
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
    rets = node.fit(df)
    # Save the result.
    rets_df = rets["df_out"]
    return rets_df


# %% [markdown]
# # Load the data

# %% [markdown]
# ## CCXT

# %% [markdown]
# ### Real-time

# %%
# Specify params.
vendor = config["data"]["vendor"]
resample_1min = True
connection = config["load"]["connection"]
# Initiate the client.
ccxt_rt_client = icdcl.CcxtCddDbClient(vendor, resample_1min, connection)

# %% [markdown]
# #### Universe

# %%
# Specify the universe.
rt_universe_ccxt = ccxt_rt_client.get_universe()
len(rt_universe_ccxt)

# %%
# Choose cc for analysis.
full_symbols = rt_universe_ccxt[0:2]
full_symbols

# %% [markdown]
# #### Data Loader

# %%
# Specify time period.
start_date = config["data"]["start_date_rt"]
end_date = config["data"]["end_date_rt"]

# Load the data.
data_rt_ccxt = ccxt_rt_client.read_data(full_symbols, start_date, end_date)
display(data_rt_ccxt.shape)
display(data_rt_ccxt.head(3))

# %% [markdown]
# ### Historical

# %%
# Specify params.
resample_1min = True
root_dir = config["load"]["data_dir_hist"]
partition_mode = config["load"]["partition_mode"]
data_snapshot = config["load"]["data_snapshot"]
aws_profile = config["load"]["aws_profile"]

# Initiate the client.
historical_client_ccxt = icdcl.CcxtHistoricalPqByTileClient(
    resample_1min,
    root_dir,
    partition_mode,
    data_snapshot=data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# #### Universe

# %%
# Specify the universe.
historical_universe = historical_client_ccxt.get_universe()
len(historical_universe)

# %%
# Choose cc for analysis.
full_symbols = historical_universe[0:2]
full_symbols

# %% [markdown]
# #### Data Loader

# %%
# Specify time period.
start_date = config["data"]["start_date_hist"]
end_date = config["data"]["end_date_hist"]

# Load the data.
data_hist_ccxt = historical_client_ccxt.read_data(
    full_symbols, start_date, end_date
)
display(data_hist_ccxt.shape)
display(data_hist_ccxt.head(3))

# %% [markdown]
# ## Talos

# %% [markdown]
# ### Real-time

# %%
# Specify params.
resample_1min = True
db_connection = config["load"]["connection"]
table_name = "talos_ohlcv"

talos_rt_client = imvtdctacl.RealTimeSqlTalosClient(
    resample_1min, db_connection, table_name
)

# %% [markdown]
# #### Universe

# %%
# Specify the universe.
rt_universe_talos = sorted(talos_rt_client.get_universe())
len(rt_universe_talos)

# %%
# Choose cc for analysis.
full_symbols = rt_universe_talos[0:2]
full_symbols

# %% [markdown]
# #### Data Loader

# %%
# Specify the period.
start_date = config["data"]["start_date_rt"]
end_date = config["data"]["end_date_rt"]

# Load the data.
data_rt_talos = talos_rt_client.read_data(full_symbols, start_date, end_date)
display(data_rt_talos.shape)
display(data_rt_talos.head(3))

# %% [markdown]
# ### Historical

# %%
# Specify params.
resample_1min = True
root_dir = config["load"]["data_dir_hist"]
partition_mode = config["load"]["partition_mode"]
data_snapshot = config["load"]["data_snapshot"]
aws_profile = config["load"]["aws_profile"]

talos_hist_client = imvtdctacl.TalosHistoricalPqByTileClient(
    resample_1min,
    root_dir,
    partition_mode,
    data_snapshot=data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# #### Universe

# %%
# Specify the universe.
hist_universe_talos = talos_hist_client.get_universe()
len(hist_universe_talos)

# %%
# Choose cc for analysis.
full_symbols_hist_talos = hist_universe_talos[0:2]
full_symbols_hist_talos

# %% [markdown]
# #### Data Loader

# %% run_control={"marked": false}
# Specify the period.
start_date = config["data"]["start_date_hist"]
end_date = config["data"]["end_date_hist"]

# Load the data.
data_hist_talos = talos_hist_client.read_data(
    full_symbols_hist_talos, start_date, end_date
)
# Hardcoded solution to convert OHLCV to the 'float' type for the further use.
for cols in data_hist_talos.columns[1:]:
    data_hist_talos[cols] = data_hist_talos[cols].astype(float)
# Show the data.
display(data_hist_talos.shape)
display(data_hist_talos.head(3))

# %% [markdown]
# # Calculate VWAP, TWAP and returns in `Dataflow` style

# %% [markdown]
# ## CCXT

# %%
# VWAP, TWAP transformation.
resampling_rule = config["transform"]["resampling_rule"]
vwap_twap_df = calculate_vwap_twap(data_hist_ccxt, resampling_rule)

# Returns calculation.
rets_type = config["transform"]["rets_type"]
vwap_twap_rets_df = calculate_returns(vwap_twap_df, rets_type)

# %% run_control={"marked": false}
# Show the snippet.
vwap_twap_rets_df.head(3)

# %% run_control={"marked": false}
# Stats and vizualisation to check the outcomes.
ada_ex = vwap_twap_rets_df.swaplevel(axis=1)
ada_ex = ada_ex["binance::ADA_USDT"][["close.ret_0", "twap.ret_0", "vwap.ret_0"]]
display(ada_ex.corr())
ada_ex.plot()

# %% [markdown]
# ## Talos

# %%
# VWAP, TWAP transformation.
resampling_rule = config["transform"]["resampling_rule"]
vwap_twap_df_talos = calculate_vwap_twap(data_hist_talos, resampling_rule)

# Returns calculation.
rets_type = config["transform"]["rets_type"]
vwap_twap_rets_df_talos = calculate_returns(vwap_twap_df_talos, rets_type)

# %%
# Show the snippet.
vwap_twap_rets_df_talos.head(3)

# %%
# Stats and vizualisation to check the outcomes.
ada_ex = vwap_twap_rets_df_talos.swaplevel(axis=1)
ada_ex = ada_ex["binance::ADA_USDT"][["close.ret_0", "twap.ret_0", "vwap.ret_0"]]
display(ada_ex.corr())
ada_ex.plot()
