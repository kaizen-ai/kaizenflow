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
from typing import List

import pandas as pd
import requests

import core.config.config_ as cconconf
import core.finance as cofinanc
import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import core.plotting.normality as cplonorm
import dataflow.core as dtfcore
import dataflow.model.stats_computer as dtfmostcom
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.common.universe as ivcu
import core.explore as coexplor
import core.plotting.plotting_utils as cplpluti

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_cmtask1704_config_crypto_chassis() -> cconconf.Config:
    """
    Get config, that specifies params for getting raw data from `crypto
    chassis`.
    """
    config = cconconf.Config()
    # Load parameters.
    # config.add_subconfig("load")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["full_symbols"] = ["binance::BNB_USDT", "binance::BTC_USDT"]
    config["data"]["start_date"] = pd.Timestamp("2022-01-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2022-01-15", tz="UTC")
    # Transformation parameters.
    config.add_subconfig("transform")
    config["transform"]["resampling_rule"] = "5T"
    config["transform"]["rets_type"] = "pct_change"
    return config


# %%
config = get_cmtask1704_config_crypto_chassis()
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
def get_exchange_currency_for_api_request(full_symbol: str) -> str:
    """
    Returns `exchange_id` and `currency_pair` in a format for requests to cc
    API.
    """
    cc_exchange_id, cc_currency_pair = ivcu.parse_full_symbol(full_symbol)
    cc_currency_pair = cc_currency_pair.lower().replace("_", "-")
    return cc_exchange_id, cc_currency_pair


def load_crypto_chassis_ohlcv_for_one_symbol(full_symbol: str) -> pd.DataFrame:
    """
    - Transform CK `full_symbol` to the `crypto-chassis` request format.
    - Construct OHLCV data request for `crypto-chassis` API.
    - Save the data as a DataFrame.
    """
    # Deconstruct `full_symbol`.
    cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(
        full_symbol
    )
    # Build a request.
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{cc_exchange_id}/{cc_currency_pair}?startTime=0"
    )
    # Get url with data.
    url = r.json()["historical"]["urls"][0]["url"]
    # Read the data.
    df = pd.read_csv(url, compression="gzip")
    return df


def apply_ohlcv_transformation(
    df: pd.DataFrame,
    full_symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
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
    df = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    return df


def read_crypto_chassis_ohlcv(
    full_symbols: List[str], start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
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
def get_list_of_dates_for_period(
    start_date: pd.Timestamp, end_date: pd.Timestamp
) -> str:
    """
    Since cc API only loads the data for one day, on need to get all the
    timestamps for days in the interval.
    """
    # Get the list of all dates in the range.
    num_of_periods = (end_date - start_date).days
    datelist = pd.date_range(start_date, periods=num_of_periods).tolist()
    datelist = [str(x.strftime("%Y-%m-%d")) for x in datelist]
    return datelist


def load_bid_ask_data_for_one_symbol(
    full_symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """
    For each date inside the period load the bid-ask data.
    """
    # Using the variables from `datelist` the multiple requests can be sent to the API.
    list_of_dates = get_list_of_dates_for_period(start_date, end_date)
    result = []
    for date in list_of_dates:
        # Deconstruct `full_symbol` for API request.
        cc_exchange_id, cc_currency_pair = get_exchange_currency_for_api_request(
            full_symbol
        )
        # Interaction with the API.
        r = requests.get(
            f"https://api.cryptochassis.com/v1/market-depth/{cc_exchange_id}/{cc_currency_pair}?startTime={date}"
        )
        data = pd.read_csv(r.json()["urls"][0]["url"], compression="gzip")
        # Attaching it day-by-day to the final DataFrame.
        result.append(data)
    bid_ask_df = pd.concat(result)
    return bid_ask_df


def apply_bid_ask_transformation(
    df: pd.DataFrame, full_symbol: str
) -> pd.DataFrame:
    """
    - Divide (price, size) columns.
    - Convert timestamps and set them as index.
    - Convert data columns to `float`.
    - Add `full_symbol` column.
    """
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


def resample_bid_ask(df: pd.DataFrame, resampling_rule: str) -> pd.DataFrame:
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


def read_and_resample_bid_ask_data(
    full_symbols: List[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    resampling_rule: str,
) -> pd.DataFrame:
    """
    General method that does:

    - Data loading
    - Transformation to CK format
    - Resampling
    """
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


def calculate_bid_ask_statistics(df: pd.DataFrame) -> pd.DataFrame:
    # Convert to multiindex.
    converted_df = dtfsysonod._convert_to_multiindex(df, "full_symbol")
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
    # Convert relative spread to bps.
    bid_ask_metrics["relative_spread"] = bid_ask_metrics["relative_spread"]*10000
    return bid_ask_metrics


# %%
# Specify the params.
full_symbols = config["data"]["full_symbols"]
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]
# Get the data.
bid_ask_df = read_and_resample_bid_ask_data(
    full_symbols, start_date, end_date, "5T"
)
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
final_df[["relative_spread"]].plot()

# %% [markdown]
# ## Compute the distribution of (return - spread)

# %%
# Choose the specific `full_symbol`.
df_bnb = final_df.swaplevel(axis=1)["binance::BNB_USDT"]
df_bnb.head(3)

# %%
# Calculate (|returns| - spread) and display descriptive stats.
df_bnb["ret_spr_diff"] = abs(df_bnb["close.ret_0"]) - (df_bnb["quoted_spread"]/df_bnb["close"])
display(df_bnb["ret_spr_diff"].describe())

# %%
# Visualize the result
cplonorm.plot_qq(df_bnb["ret_spr_diff"])

# %% [markdown]
# # Deep dive into quantitative statistics #1805

# %% [markdown]
# ## Check that our VWAP and TWAP match the version reported by Chassis

# %% run_control={"marked": false}
# Load minutely OHLCV data from crypto-chassis (so we don't corrupt initial vwap, twap).
# Time interval = 2 years
full_symbol = ["binance::BTC_USDT"]
start_date = pd.Timestamp("2020-01-01", tz="UTC")
end_date = pd.Timestamp("2022-01-01", tz="UTC")
df = read_crypto_chassis_ohlcv(full_symbol, start_date, end_date)

# %%
# VWAP, TWAP transformation.
resampling_rule = "1T"
vwap_twap_df = calculate_vwap_twap(df, resampling_rule)

# %%
# Construct DataFrame with VWAP, TWAP from different sources.
# _chassis - vwap,twap from raw data.
cc_vwap = df[["vwap", "twap"]]
cc_vwap = cc_vwap.add_suffix('_chassis')
# _ck - vwap,twap calculated with the nodes.
ck_vwap = vwap_twap_df.swaplevel(axis=1)["binance::BTC_USDT"][["vwap", "twap"]]
ck_vwap = ck_vwap.add_suffix('_ck')
# Unique DataFrame.
ols_df = pd.concat([cc_vwap, 
                    ck_vwap],axis=1)

# %%
# OLS VWAP.
predicted_var = "vwap_chassis"
predictor_vars = "vwap_ck"
intercept = False
# Run OLS.
coexplor.ols_regress(
    ols_df,
    predicted_var,
    predictor_vars,
    intercept,
)

# %%
# OLS TWAP.
predicted_var = "twap_chassis"
predictor_vars = "twap_ck"
intercept = False
# Run OLS.
coexplor.ols_regress(
    ols_df,
    predicted_var,
    predictor_vars,
    intercept,
)

# %%
display(ols_df.corr())

# %% [markdown]
# ### Summary

# %% [markdown]
# Judging by the numbers above, I think it's fair to say that the vwap,twap from raw data is almost a perfect match with the ones computed with internal methods.

# %% [markdown]
# ## How much liquidity is available at the top of the book?

# %%
liquidity_stats = (final_df["ask_size"] * final_df["ask_price"]).median()
display(liquidity_stats)
cplpluti.plot_barplot(liquidity_stats)


# %% [markdown]
# ## Is the quoted spread constant over the day?

# %%
def plot_overtime_spread(coin_df, resampling_rule, num_stds=1):
    df = cfinresa.resample(coin_df, rule=resampling_rule)["quoted_spread"].mean().to_frame()
    df["time"] = df.index.time
    mean = df.groupby("time")["quoted_spread"].mean()
    std = df.groupby("time")["quoted_spread"].std()
    (mean + num_stds * std).plot(color="blue")
    mean.plot(lw=2, color="black")
    #(mean - num_stds * std).plot(color="blue")
    return df


# %%
#full_symbol = "binance::BNB_USDT"
full_symbol = "binance::BTC_USDT"
data = final_df.swaplevel(axis=1)[full_symbol]
dd = plot_overtime_spread(data, "10T")
display(dd.head())

# %%

# %%

# %% [markdown]
# ## - Compute some high-level stats (e.g., median relative spread, median bid / ask notional, volatility, volume) by coins

# %%
high_level_stats = pd.DataFrame()
high_level_stats["median_relative_spread"] = final_df["relative_spread"].median()
high_level_stats["median_notional_bid"] = final_df["bid_value"].median()
high_level_stats["median_notional_ask"] = final_df["ask_value"].median()
high_level_stats["median_notional_volume"] = (final_df["volume"]*final_df["close"]).median()
high_level_stats["volatility_for_period"] = final_df['close.ret_0'].std()*final_df.shape[0]**0.5

high_level_stats.head(3)
