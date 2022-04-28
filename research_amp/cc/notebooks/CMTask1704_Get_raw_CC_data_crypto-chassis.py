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
import logging
import os

import pandas as pd
import requests

import core.config.config_ as cconconf
import core.finance as cofinanc
import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import core.plotting.normality as cplonorm
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita
import im_v2.talos.data.client.talos_clients as imvtdctacl

import im_v2.common.universe as ivcu

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
    #config.add_subconfig("load")
    #env_file = imvimlita.get_db_env_path("dev")
    #connection_params = hsql.get_connection_info_from_env_file(env_file)
    #config["load"]["connection"] = hsql.get_connection(*connection_params)
    #config["load"]["aws_profile"] = "ck"
    #config["load"]["data_dir_hist"] = os.path.join(
    #    "s3://cryptokaizen-data", "historical"
    #)
    #config["load"]["data_snapshot"] = "latest"
    #config["load"]["partition_mode"] = "by_year_month"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["full_symbols"] = ['binance::BNB_USDT', 'binance::BTC_USDT']
    config["data"]["start_date"] = pd.Timestamp("2022-01-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2022-01-15", tz="UTC")
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
# # Load OHLCV data from `crypto-chassis`

# %%
# TODO(Max): Refactor the loading part once #1766 is implemented.

# %% [markdown]
# ## Functions to extract and process data

# %%
def get_exchange_currency_for_api_request(full_symbol):
    cc_exchange_id, cc_currency_pair = ivcu.parse_full_symbol(full_symbol)
    cc_currency_pair = cc_currency_pair.lower().replace("_", "-")
    return cc_exchange_id, cc_currency_pair

def load_crypto_chassis_ohlcv_for_one_symbol(full_symbol):
    """
    - Transform CK `full_symbol` to the `crypto-chassis` request format.
    - Construct OHLCV data request for `crypto-chassis` API.
    - Save the data as a DataFrame.
    """
    # Deconstruct `full_symbol`.
    cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(full_symbol)
    # Build a request.
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{cc_exchange_id}/{cc_currency_pair}?startTime=0"
    )
    # Get url with data.
    url = r.json()["historical"]["urls"][0]["url"]
    # Read the data.
    df = pd.read_csv(url, compression="gzip")
    return df

def apply_ohlcv_transformation(df, full_symbol, start_date, end_date):
    """
    The following transformations are applied:
    - Convert `timestamps` to the usual format.
    - Convert data columns to `float`.
    - Add `full_symbol` column.
    """
    # Convert `timestamps` to the usual format.
    df = df.rename(columns={"time_seconds": "timestamp"})
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("timestamp")
    # Convert to `float`.
    for cols in df.columns:
        df[cols] = df[cols].astype(float)
    # Add `full_symbol`.
    df["full_symbol"] = full_symbol
    # Note: I failed to put [start_time, end_time] to historical request.
    # Now it loads all the available data.
    # For that reason the time interval is hardcoded on this stage.
    df = df.loc[(df.index>=start_date)&(df.index<=end_date)]
    return df

def read_crypto_chassis_ohlcv(full_symbols, start_date, end_date):
    """
    - Load the raw data for one symbol.
    - Convert it to CK format.
    - Repeat the first two steps for all `full_symbols`.
    - Concentrate them into unique DataFrame.
    """
    result = []
    for full_symbol in full_symbols:
        # Load raw data.
        df_raw = load_crypto_chassis_ohlcv_for_one_symbol(full_symbol)
        # Process it to CK format.
        df = apply_ohlcv_transformation(df_raw, full_symbol, start_date, end_date)
        result.append(df)
    final_df = pd.concat(result)
    return final_df


# %% [markdown]
# ## Data demonstration

# %%
full_symbols = config["data"]["full_symbols"]
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]

ohlcv_cc = read_crypto_chassis_ohlcv(full_symbols, start_date, end_date)

# %%
ohlcv_cc.head(3)

# %% [markdown]
# # Calculate VWAP, TWAP and returns in `Dataflow` style

# %%
# VWAP, TWAP transformation.
resampling_rule = config["transform"]["resampling_rule"]
vwap_twap_df = calculate_vwap_twap(ohlcv_cc, resampling_rule)

# Returns calculation.
rets_type = config["transform"]["rets_type"]
vwap_twap_rets_df = calculate_returns(vwap_twap_df, rets_type)

# %% run_control={"marked": false}
# Show the snippet.
vwap_twap_rets_df.head(3)

# %% run_control={"marked": false}
# Stats and vizualisation to check the outcomes.
bnb_ex = vwap_twap_rets_df.swaplevel(axis=1)
bnb_ex = bnb_ex["binance::BNB_USDT"][["close.ret_0", "twap.ret_0", "vwap.ret_0"]]
display(bnb_ex.corr())
bnb_ex.plot()


# %% [markdown]
# # Bid-ask data

# %%
# TODO(Max): Refactor the loading part once #1766 is implemented.

# %%
def get_list_of_dates_for_period(start_date, end_date):
    # Get the list of all dates in the range.
    num_of_periods = (end_date-start_date).days
    datelist = pd.date_range(start_date, periods=num_of_periods).tolist()
    datelist = [str(x.strftime("%Y-%m-%d")) for x in datelist]
    return datelist

def load_bid_ask_data_for_one_symbol(full_symbol, start_date, end_date):
    # Using the variables from `datelist` the multiple requests can be sent to the API.
    list_of_dates = get_list_of_dates_for_period(start_date, end_date)
    result = []
    for date in list_of_dates:
        # Deconstruct `full_symbol` for API request.
        cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(full_symbol)
        # Interaction with the API.
        r = requests.get(
            f"https://api.cryptochassis.com/v1/market-depth/{cc_exchange_id}/{cc_currency_pair}?startTime={date}"
        )
        data = pd.read_csv(r.json()["urls"][0]["url"], compression="gzip")
        # Attaching it day-by-day to the final DataFrame.
        result.append(data)
    bid_ask_df = pd.concat(result)
    return bid_ask_df

def apply_bid_ask_transformation(df, full_symbol):
    # Split the columns to differentiate between `price` and `size`.
    df[["bid_price", "bid_size"]] = df["bid_price_bid_size"].str.split(
        "_", expand=True
    )
    df[["ask_price", "ask_size"]] = df["ask_price_ask_size"].str.split(
        "_", expand=True
    )
    df = df.drop(columns=["bid_price_bid_size", "ask_price_ask_size"])
    # Convert `timestamps` to the usual format.
    df = df.rename(columns={"time_seconds": "timestamp"})
    df["timestamp"] = df["timestamp"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("timestamp")
    # Convert to `float`.
    for cols in df.columns:
        df[cols] = df[cols].astype(float)
    # Add `full_symbol` column.
    df["full_symbol"] = full_symbol
    return df

def resample_bid_ask(df, resampling_rule):
    """
    In the current format the data is presented in the `seconds` frequency. In
    order to convert it to the minutely (or other) frequencies the following
    aggregation rules are applied:

    - Size is the sum of all sizes during the resampling period
    - Price is the mean of all prices during the resampling period
    """
    new_df = cfinresa.resample(df, rule=resampling_rule).agg(
        {
            "bid_price": "mean",
            "bid_size": "sum",
            "ask_price": "mean",
            "ask_size": "sum",
            "full_symbol": "last",
        }
    )
    return new_df

def read_and_resample_bid_ask_data(full_symbols, start_date, end_date, resampling_rule):
    result = []
    for full_symbol in full_symbols:
        # Load raw bid ask data.
        df = load_bid_ask_data_for_one_symbol(full_symbol, start_date, end_date)
        # Apply transformation.
        df = apply_bid_ask_transformation(df, full_symbol)
        # Resample the data.
        df = resample_bid_ask(df, resampling_rule)
        result.append(df)
    bid_ask_df = pd.concat(result) 
    return bid_ask_df

def calculate_bid_ask_statistics(df):
    # Convert to multiindex.
    converted_df = dtfsysonod._convert_to_multiindex(
        df, "full_symbol"
    )
    # Configure the node to calculate the returns.
    node_bid_ask_config = {
        "in_col_groups": [
            ("ask_price",),
            ("ask_size",),
            ("bid_price",),
            ("bid_size",),
        ],
        "out_col_group": (),
        "transformer_kwargs": {
            "bid_col": "bid_price",
            "ask_col": "ask_price",
            "bid_volume_col": "bid_size",
            "ask_volume_col": "ask_size",
        },
    }
    # Create the node that computes bid ask metrics.
    nid = "process_bid_ask"
    node = dtfcore.GroupedColDfToDfTransformer(
        nid,
        transformer_func=cfibiask.process_bid_ask,
        **node_bid_ask_config,
    )
    # Compute the node on the data.
    bid_ask_metrics = node.fit(converted_df)
    # Save the result.
    bid_ask_metrics = bid_ask_metrics["df_out"]
    return bid_ask_metrics


# %%
full_symbols = config["data"]["full_symbols"]
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]

bid_ask_df = read_and_resample_bid_ask_data(full_symbols, start_date, end_date, "5T")
bid_ask_df.head(3)

# %%
# Calculate bid-ask metrics.
bid_ask_df = calculate_bid_ask_statistics(bid_ask_df)
bid_ask_df.tail(3)

# %% [markdown]
# ## Unite VWAP, TWAP, rets statistics with bid-ask stats

# %%
final_df = pd.concat([vwap_twap_rets_df, bid_ask_df], axis=1)
final_df.tail()

# %%
# Metrics visualizations.
final_df.swaplevel(axis=1)["binance::BNB_USDT"][["quoted_spread"]].plot()
final_df.swaplevel(axis=1)["binance::BNB_USDT"][["relative_spread"]].plot()

# %% [markdown]
# ## Compute the distribution of (return - spread)

# %%
# Choose the specific `full_symbol`.
df_bnb = final_df.swaplevel(axis=1)["binance::BNB_USDT"]
df_bnb.head(3)

# %%
# Calculate (|returns| - spread) and display descriptive stats.
df_bnb["ret_spr_diff"] = abs(df_bnb["close.ret_0"]) - df_bnb["quoted_spread"]
display(df_bnb["ret_spr_diff"].describe())

# %%
# Visualize the result
cplonorm.plot_qq(df_bnb["ret_spr_diff"])
