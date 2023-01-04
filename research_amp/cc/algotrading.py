"""
Import as:

import research_amp.cc.algotrading as ramccalg
"""

import logging
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.config as cconfig
import dataflow.universe as dtfuniver
import helpers.hdbg as hdbg
import helpers.hlogging as hloggin
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.crypto_chassis.data.client as iccdc
import market_data as mdata

_LOG = logging.getLogger(__name__)


# #############################################################################
# Notebook Config examples
# #############################################################################


def get_default_config(
    *,
    start_ts: Optional[pd.Timestamp] = pd.Timestamp("2022-12-14 00:00:00+00:00"),
    end_ts: Optional[pd.Timestamp] = pd.Timestamp("2022-12-15 00:00:00+00:00"),
) -> cconfig.Config:
    """
    Get a config for a notebook with algorithmic trading experiments.

    - Latest universe (v3)
    - Resampled to 1sec
    - For 1 asset and 1 day
    """
    dict_ = {
        "client_config": {
            "resample_1min": False,
            "tag": "downloaded_1sec",
            "contract_type": "futures",
            "universe": {
                "full_symbols": ["binance::ADA_USDT"],
                "universe_version": "v3",
            },
        },
        "market_data_config": {"start_ts": start_ts, "end_ts": end_ts},
    }
    config = cconfig.Config.from_dict(dict_)
    return config


# #############################################################################
# Client and MarketData initialization
# #############################################################################


def get_bid_ask_ImClient(config: cconfig.Config) -> icdc.ImClient:
    """
    Get a historical client for bid/ask data.
    """
    # Set up the parameters for initialization of the IM Client.
    universe_version = config.get_and_mark_as_used(
        ("client_config", "universe", "universe_version")
    )
    resample_1min = config.get_and_mark_as_used(
        ("client_config", "resample_1min")
    )
    contract_type = config.get_and_mark_as_used(
        ("client_config", "contract_type")
    )
    tag = config.get_and_mark_as_used(("client_config", "tag"))
    client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
        universe_version, resample_1min, contract_type, tag
    )
    config["client_config"]["client"] = client
    return client


def get_universe(config: cconfig.Config) -> List[int]:
    """
    Load asset IDs based on config universe and symbols.
    """
    universe_version = config.get_and_mark_as_used(
        ("client_config", "universe", "universe_version")
    )
    client = config.get_and_mark_as_used(("client_config", "client"))
    # Verify that provided symbols are present in the client.
    universe_string = f"crypto_chassis_{universe_version}-all"
    universe_full_symbols = dtfuniver.get_universe(universe_string)
    config_full_symbols = config.get_and_mark_as_used(
        ("client_config", "universe", "full_symbols")
    )
    hdbg.dassert_is_subset(config_full_symbols, universe_full_symbols)
    # Convert to asset ids.
    config["client_config"]["universe"][
        "asset_ids"
    ] = client.get_asset_ids_from_full_symbols(config_full_symbols)
    asset_ids = config.get_and_mark_as_used(
        ("client_config", "universe", "asset_ids")
    )
    return asset_ids


def get_market_data(config: cconfig.Config) -> mdata.MarketData:
    """
    Get historical market data to connect to data source node.
    """
    im_client = config.get_and_mark_as_used(("client_config", "client"))
    asset_ids = config.get_and_mark_as_used(
        ("client_config", "universe", "asset_ids")
    )
    columns = None
    columns_remap = None
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    market_data = mdata.get_HistoricalImClientMarketData_example1(
        im_client,
        asset_ids,
        columns,
        columns_remap,
        wall_clock_time=wall_clock_time,
    )
    return market_data


# #############################################################################
# Data Augmentation utilities
# #############################################################################


def add_limit_order_prices(
    df: pd.DataFrame,
    mid_col_name: str,
    debug_mode: bool,
    *,
    resample_freq: Optional[str] = "1T",
    passivity_factor: Optional[float] = None,
    abs_spread: Optional[float] = None,
) -> pd.DataFrame:
    """
    Calculate limit order prices for buy/sell.

    The limit order can be calculated via passivity factor or absolute spread,
    but not both.

    :param df: bid/ask DataFrame
    :param mid_col_name: name of column containing bid/ask mid price
    :param debug_mode: whether to show DataFrame info
    :param resample_freq: resampling frequency, e.g. '1T', '5T'
    :param passivity_factor: mid price factor for limit, value between 0 and 1
    :param abs_spread: value to add to a spread
    :return: original DataFrame with added limit price columns
    """
    hdbg.dassert_in(mid_col_name, df.columns.to_list())
    hdbg.dassert_is_subset(["ask_price", "bid_price"], df.columns.to_list())
    # Verify that DataFrame is in the correct format.
    original_log_level = _LOG.getEffectiveLevel()
    if debug_mode:
        hloggin.set_level(_LOG, "DEBUG")
    _LOG.debug("df initial=%s", df.shape)
    # Select mid price columns to transform.
    limit_buy_col = "limit_buy_price"
    limit_sell_col = "limit_sell_price"
    limit_buy_srs = df[mid_col_name]
    limit_buy_srs = limit_buy_srs.rename(limit_buy_col)
    limit_sell_srs = df[mid_col_name]
    limit_sell_srs = limit_sell_srs.rename(limit_buy_col)
    # Resample if necessary.
    if resample_freq:
        limit_buy_srs = limit_buy_srs.resample(resample_freq)
        limit_sell_srs = limit_sell_srs.resample(resample_freq)
    # Get mid price avg and shift by 1 period.
    limit_buy_srs = limit_buy_srs.mean().shift(1)
    limit_sell_srs = limit_sell_srs.mean().shift(1)
    # Apply passivity factor or absolute spread.
    if abs_spread is not None and passivity_factor is None:
        limit_buy_srs = limit_buy_srs - abs_spread
        limit_sell_srs = limit_sell_srs + abs_spread
    #
    elif passivity_factor is not None and abs_spread is None:
        hdbg.dassert_lgt(0, passivity_factor, 1, True, True)
        limit_buy_srs = limit_buy_srs * (1 - passivity_factor)
        limit_sell_srs = limit_sell_srs * (1 - passivity_factor)
    else:
        raise ValueError(
            "Either `passivity_factor` or `abs_spread` should be provided."
        )
    # Merge original dataframe with limit prices.
    #
    df_limit_price = pd.DataFrame()
    df_limit_price[limit_buy_col] = limit_buy_srs
    df_limit_price[limit_sell_col] = limit_sell_srs
    _LOG.debug(
        "df_limit_price after resampling and shift=%s", "df_limit_price.shape"
    )
    df = df.merge(df_limit_price, right_index=True, left_index=True, how="outer")
    _LOG.debug("df after merge=%s", df.shape)
    # Forward fill gaps if limit prices were resampled.
    #  Note: we expect the original data to be 1 second, and e.g. 1min limit
    #  price is applied to each second of that period.
    df[limit_buy_col] = df[limit_buy_col].ffill()
    df[limit_sell_col] = df[limit_sell_col].ffill()
    # Set whether the price has hit the limit.
    df["is_buy"] = df["ask_price"] <= df[limit_buy_col]
    df["is_sell"] = df["bid_price"] >= df[limit_sell_col]
    # Turn the logging level back on.
    hloggin.set_level(_LOG, original_log_level)
    return df


def compute_repricing_df(df: pd.DataFrame, report_stats: bool) -> pd.DataFrame:
    """
    Compute the execution prices.

    :param df: DataFrame containing bid/ask data
    :param report_stats: print DaraFrame stats
    :return: DataFrame with buy/sell execution prices
    """
    hdbg.dassert_is_subset(
        ["is_buy", "is_sell", "ask_price", "bid_price"], df.columns
    )
    # Calculate buy execution price.
    # is_buy * ask_price.
    df["exec_buy_price"] = df["is_buy"] * df["ask_price"]
    mask = ~df["is_buy"]
    df["exec_buy_price"][mask] = np.nan
    # Calculate sell execution price.
    # is_sell * bid_price.
    df["exec_sell_price"] = df["is_sell"] * df["bid_price"]
    mask = ~df["is_sell"]
    df["exec_sell_price"][mask] = np.nan
    # Display stats.
    if report_stats:
        # Report buy/sell percentages.
        _LOG.info(
            "buy percentage at repricing freq: %s",
            hprint.perc(df["is_buy"].sum(), df.shape[0]),
        )
        _LOG.info(
            "sell percentage at repricing freq: %s",
            hprint.perc(df["is_sell"].sum(), df.shape[0]),
        )
        # Display zero execution prices as a percentage.
        _LOG.info(
            "exec_buy_price=0 =%s",
            hprint.perc(df["exec_buy_price"].isnull().sum(), df.shape[0]),
        )
        _LOG.info(
            "exec_sell_price=0 =%s",
            hprint.perc(df["exec_sell_price"].isnull().sum(), df.shape[0]),
        )
    return df


def compute_execution_df(
    df: pd.DataFrame, report_stats: bool, *, join_output_with_input: bool = False
) -> pd.DataFrame:
    """
    Compute the number and volume of buy/sell executions.

    :param df: DataFrame with bid/ask columns
    :param report_stats: print stats on the calculated values
    :param join_output_with_input: merge the values with input DataFrame
    :return: DataFrame with execution stats
    """
    hdbg.dassert_is_subset(
        ["is_buy", "is_sell", "ask_size", "ask_price", "mid"],
        df.columns.to_list(),
    )
    exec_df = pd.DataFrame()
    #
    # Count how many "buy" executions there were in an interval.
    exec_df["exec_buy_num"] = df["is_buy"].resample("5T").sum()
    exec_df["exec_buy_price"] = df["exec_buy_price"].resample("5T").mean()
    exec_df["exec_is_buy"] = exec_df["exec_buy_num"] > 0
    if report_stats:
        _LOG.info(
            "exec_is_buy=%s",
            hprint.perc(
                exec_df["exec_is_buy"].sum(),
                exec_df["exec_is_buy"].shape[0],
            ),
        )
    # Estimate the executed "buy" volume.
    exec_df["exec_buy_volume"] = (
        (df["ask_size"] * df["ask_price"] * df["is_buy"]).resample("5T").sum()
    )
    if report_stats:
        _LOG.info(
            "million USD per 5T=%s", exec_df["exec_buy_volume"].mean() / 1e6
        )
    #
    # Count how many "sell" executions there were in an interval.
    exec_df["exec_sell_num"] = df["is_sell"].resample("5T").sum()
    exec_df["exec_sell_price"] = df["exec_sell_price"].resample("5T").mean()
    exec_df["exec_is_sell"] = exec_df["exec_sell_num"] > 0
    if report_stats:
        _LOG.info(
            "exec_is_sell=%s",
            hprint.perc(
                exec_df["exec_is_sell"].sum(), exec_df["exec_is_sell"].shape[0]
            ),
        )
    # Estimate the executed "sell" volume.
    exec_df["exec_sell_volume"] = (
        (df["bid_size"] * df["bid_price"] * df["is_sell"]).resample("5T").sum()
    )
    if report_stats:
        _LOG.info(
            "million USD per 5T=%s", exec_df["exec_sell_volume"].mean() / 1e6
        )
    #
    # Join original DF with execution price, if required.
    if join_output_with_input:
        exec_df = exec_df.merge(
            df, right_index=True, left_index=True, how="outer"
        )
    else:
        exec_df["mid"] = df["mid"]
    return exec_df


def perform_spread_analysis(
    df: pd.DataFrame,
    ask_price_col_name: str,
    bid_price_col_name: str,
    mid_price_col_name: str,
) -> None:
    """
    Plot and display spread values.

    The values are displayed as absolute and in BPS.
    """
    # Get spread based on ask and bid prices.
    spread = df[ask_price_col_name] - df[bid_price_col_name]
    spread_in_bps = spread / df[mid_price_col_name] * 1e4
    #
    # Initialize subplots.
    _, axs = plt.subplots(3, figsize=(15, 15))
    # Plot spread BPS histogram.
    axs[0].hist(spread_in_bps, bins=101)
    axs[0].set_title("Spread in BPS histogram")
    # Plot absolute spread values.
    axs[1].plot(spread)
    axs[1].set_title("Absolute spread")
    # Plot BPS spread values.
    axs[2].plot(spread_in_bps)
    axs[2].set_title("Spread in BPS")


def plot_limit_orders(
    df: pd.DataFrame,
    *,
    start_timestamp: Optional[pd.Timestamp] = None,
    end_timestamp: Optional[pd.Timestamp] = None,
) -> None:
    """
    Plot limit orders data.

    The given timestamp range is plotted. If no timestamps displayed,
    the first 1000 data points are plotted.

    :param df: DataFrame containing limit orders data
    """
    hdbg.dassert_is_subset(
        [
            "mid",
            "ask_price",
            "bid_price",
            "limit_buy_price",
            "limit_sell_price",
            "is_buy",
            "is_sell",
        ],
        df.columns.to_list(),
    )
    #
    # Select relevant columnsю
    price_data = df[
        ["mid", "ask_price", "bid_price", "limit_buy_price", "limit_sell_price"]
    ]
    is_trade_data = df[["is_buy", "is_sell"]]
    #
    # Select a subset of the data based on a timestamp.
    if all([start_timestamp, end_timestamp]):
        price_data = price_data.loc[start_timestamp:end_timestamp]
        is_trade_data = is_trade_data.loc[start_timestamp:end_timestamp]
    # Select top 1000 rows.
    elif not any([start_timestamp, end_timestamp]):
        price_data = price_data.head(1000)
        is_trade_data = is_trade_data.head(1000)
    else:
        raise ValueError(
            "Either both start and end timestamp should be provided, or none."
        )
    #
    # Plot limit price data.
    _, axs = plt.subplots(2, figsize=(15, 15))
    axs[0].plot(price_data)
    axs[0].set_title("Price data")
    axs[1].plot(is_trade_data)
    axs[1].set_title("is_trade data")