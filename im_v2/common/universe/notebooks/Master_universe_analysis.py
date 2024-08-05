# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
# The notebooks performs an EDA of the given universe of assets.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import numpy as np
import pandas as pd
import scipy

import core.config as cconfig
import core.finance as cofinanc
import dataflow_amp.system.Cx as dtfamsysc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import im_v2.common.universe as ivcu
import market_data as mdata
import market_data.market_data_example as mdmadaex

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
config = cconfig.get_config_from_env()
if config:
    # Get config from env when running the notebook via the `run_notebook.py`
    # script, e.g., in the system reconciliation flow.
    _LOG.info("Using config from env vars")
else:
    universe_version = "v8"
    wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
    config = {
        "universe": {
            "vendor": "CCXT",
            "mode": "trade",
            "version": universe_version,
            "as_full_symbol": True,
        },
        "ohlcv_data": {
            "start_timestamp": pd.Timestamp("2024-03-01T00:00:00+00:00"),
            "end_timestamp": pd.Timestamp("2024-03-31T23:59:00+00:00"),
            "im_client_config": {
                "vendor": "ccxt",
                "universe_version": universe_version,
                "root_dir": "s3://cryptokaizen-data.preprod/v3",
                "resample_1min": False,
                "partition_mode": "by_year_month",
                "dataset": "ohlcv",
                "contract_type": "futures",
                "data_snapshot": "",
                "aws_profile": "ck",
                "version": "v1_0_0",
                "download_universe_version": "v8",
                "tag": "downloaded_1min",
                "download_mode": "periodic_daily",
                "downloading_entity": "airflow",
            },
            "market_data_config": {
                "columns": None,
                "column_remap": None,
                "wall_clock_time": wall_clock_time,
            },
            "column_names": {
                "close": "close",
                "volume": "volume",
            },
        },
        "bid_ask_data": {
            "start_timestamp": pd.Timestamp("2024-03-01T00:00:00+00:00"),
            "end_timestamp": pd.Timestamp("2024-03-31T00:00:00+00:00"),
            "im_client_config": {
                "universe_version": "v8",
                "root_dir": "s3://cryptokaizen-data-test/v3",
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                "data_snapshot": "",
                "version": "v2_0_0",
                "download_universe_version": "v8",
                "tag": "resampled_1min",
                "aws_profile": "ck",
            },
            "market_data_config": {
                "columns": cofinanc.get_bid_ask_columns_by_level(1)
                + ["asset_id", "full_symbol", "start_ts", "knowledge_timestamp"],
                "column_remap": None,
                "wall_clock_time": wall_clock_time,
                "filter_data_mode": "assert",
            },
            "rolling_window": 30,
            "column_names": {
                "timestamp": "timestamp",
                "full_symbol": "full_symbol",
                "close": "close",
                "volume": "volume",
                "volume_notional": "volume_notional",
                "ask_price": "level_1.ask_price.close",
                "bid_price": "level_1.bid_price.close",
                "ask_size": "level_1.ask_size.close",
                "bid_size": "level_1.bid_size.close",
                "bid_ask_midpoint": "level_1.bid_ask_midpoint.close",
                "half_spread": "level_1.half_spread.close",
            },
        },
        "US_equities_tz": "America/New_York",
        "plot_kwargs": {
            "kind": "barh",
            "logx": True,
            "figsize": (20, 100),
        },
    }
    config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# # Universe

# %%
# Get full symbol universe.
full_symbols = ivcu.get_vendor_universe(**config["universe"])
_LOG.info("number of assets=%s", len(full_symbols))

# %%
# Get asset ids.
asset_ids = [
    ivcu.string_to_numerical_id(full_symbol) for full_symbol in full_symbols
]
_LOG.info("number of asset_ids=%d", len(asset_ids))

# %%
# Get asset id to full symbol mapping.
asset_id_to_full_symbol_mapping = ivcu.build_numerical_to_string_id_mapping(
    full_symbols
)
_LOG.info("number of elements in asset id mapping=%d", len(asset_id_to_full_symbol_mapping))
asset_id_to_full_symbol_mapping

# %% [markdown]
# # Mean daily notional volume

# %%
im_client = imvcdchpcl.HistoricalPqByCurrencyPairTileClient(
    **config["ohlcv_data"]["im_client_config"]
)
market_data = mdata.get_HistoricalImClientMarketData_example1(
    im_client,
    asset_ids,
    **config["ohlcv_data"]["market_data_config"],
)

# %%
# Load OHLCV data.
ohlcv_data = dtfamsysc.load_market_data(
    market_data,
    config["ohlcv_data"]["start_timestamp"],
    config["ohlcv_data"]["end_timestamp"],
)
hpandas.df_to_str(ohlcv_data, log_level=logging.INFO)

# %%
# Compute notional volume.
volume_notional = (
    ohlcv_data[config["ohlcv_data"]["column_names"]["volume"]]
    * ohlcv_data[config["ohlcv_data"]["column_names"]["close"]]
)
hpandas.df_to_str(volume_notional, log_level=logging.INFO)

# %%
# Compute mean daily notional volume.
mdv_notional = volume_notional.resample("D").sum().mean().rename("mdv_notional")
mdv_notional = mdv_notional.sort_values(ascending=False).round(2)
# Replace asset ids with full symbols.
mdv_notional.index = [
    asset_id_to_full_symbol_mapping[idx] for idx in mdv_notional.index
]
# Full symbols with 0 MDV also have 0 volume and constant price in the observed time period.
hpandas.df_to_str(mdv_notional, log_level=logging.INFO)

# %%
title = "MDV"
ylabel = "notional"
ax = mdv_notional.plot(
    title=title,
    ylabel=ylabel,
    kind="barh",
    logx=True,
    figsize=(20, 100),
)
ax.invert_yaxis()

# %%
# Convert to ET to be able to compare with US equities active trading hours.
ohlcv_volume = ohlcv_data[
    config["ohlcv_data"]["column_names"]["volume"]
].tz_convert(config["US_equities_tz"])
ohlcv_volume = ohlcv_volume.rename(
    columns={
        col: asset_id_to_full_symbol_mapping[col] for col in ohlcv_volume.columns
    }
)
hpandas.df_to_str(ohlcv_volume, log_level=logging.INFO)

# %%
mean_hourly_volume = ohlcv_volume.groupby(lambda x: x.hour).mean()
hpandas.df_to_str(mean_hourly_volume, log_level=logging.INFO)

# %%
title = "Z-scored mean hourly notional volume"
mean_hourly_volume.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="hour",
    ylabel="z-score",
)

# %%
mean_minutely_volume = ohlcv_volume.groupby(lambda x: x.minute).mean()
hpandas.df_to_str(mean_minutely_volume, log_level=logging.INFO)

# %%
title = "Z-scored mean minutely notional volume"
mean_minutely_volume.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="minute",
    ylabel="z-score",
)

# %%
# Days of the week are numbered as follows:
# Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6.
mean_weekday_volume = ohlcv_volume.groupby(ohlcv_volume.index.weekday).mean()
hpandas.df_to_str(mean_weekday_volume.round(2), num_rows=7, log_level=logging.INFO)

# %%
title = "Z-scored mean weekday notional volume"
mean_weekday_volume.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="day of week (Monday = 0)",
    ylabel="z-score",
)

# %% [markdown]
# # Bid / ask price changes

# %%
bid_ask_im_client = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    **config["bid_ask_data"]["im_client_config"]
)

# %%
bid_ask_market_data = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client,
    asset_ids,
    **config["bid_ask_data"]["market_data_config"],
)

# %%
bid_ask_data = bid_ask_market_data.get_data_for_interval(
    config["bid_ask_data"]["start_timestamp"],
    config["bid_ask_data"]["end_timestamp"],
    config["bid_ask_data"]["column_names"]["timestamp"],
    asset_ids,
)
# Convert to ET to be able to compare with US equities active trading hours.
bid_ask_data.index = bid_ask_data.index.tz_convert(config["US_equities_tz"])
hpandas.df_to_str(bid_ask_data, log_level=logging.INFO)

# %%
# Set input parameters.
rolling_window = config["bid_ask_data"]["rolling_window"]
full_symbol_col = config["bid_ask_data"]["column_names"]["full_symbol"]
ask_price_col = config["bid_ask_data"]["column_names"]["ask_price"]
bid_price_col = config["bid_ask_data"]["column_names"]["bid_price"]
ask_size_col = config["bid_ask_data"]["column_names"]["ask_size"]
bid_size_col = config["bid_ask_data"]["column_names"]["bid_size"]
# Get ask and bid prices for all instruments.
# TODO(Dan): ideally we should use `HistoricalDataSource` so that it converts the data to the DataFlow format.
ask_price_df = bid_ask_data.pivot(columns=full_symbol_col, values=ask_price_col)
bid_price_df = bid_ask_data.pivot(columns=full_symbol_col, values=bid_price_col)
ask_size_df = bid_ask_data.pivot(columns=full_symbol_col, values=ask_size_col)
bid_size_df = bid_ask_data.pivot(columns=full_symbol_col, values=bid_size_col)

# %%
bid_ask_midpoint_col = config["bid_ask_data"]["column_names"]["bid_ask_midpoint"]
bid_ask_midpoint_df = bid_ask_data.pivot(
    columns=full_symbol_col, values=bid_ask_midpoint_col
)
hpandas.df_to_str(bid_ask_midpoint_df, log_level=logging.INFO)

# %% [markdown]
# ## Spread analysis

# %%
half_spread_col = config["bid_ask_data"]["column_names"]["half_spread"]
half_spread_df = bid_ask_data.pivot(
    columns=full_symbol_col, values=half_spread_col
)
half_spread_bps_df = 1e4 * half_spread_df / bid_ask_midpoint_df
hpandas.df_to_str(half_spread_bps_df, log_level=logging.INFO)

# %% run_control={"marked": false}
half_spread_bps_mean = half_spread_bps_df.mean().sort_values()
half_spread_bps_mean.name = "half_spread_bps_mean"
#
title = "Half bid/ask spread"
ylabel = "bps"
ax = half_spread_bps_mean.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)
ax.invert_yaxis()

# %%
# TODO: break this down by hour, minute, day-of-week. 

# %% [markdown]
# ## Vol analysis

# %%
ask_vol_df = ask_price_df.ffill().pct_change().rolling(rolling_window).std()
ask_vol_bps_df = 1e4 * ask_vol_df
hpandas.df_to_str(ask_vol_bps_df, log_level=logging.INFO)

# %%
ask_vol_bps_mean = ask_vol_bps_df.mean().sort_values()
ask_vol_bps_mean.name = "ask_vol_bps_mean"
#
if False:
    title = "ask vol"
    ylabel = "bps"
    ask_vol_bps_mean.plot(
        title=title,
        ylabel=ylabel,
        **config["plot_kwargs"],
    )

# %%
if False:
    mean_minutely_ask_vol_by_hour = ask_vol_bps_df.groupby(lambda x: x.hour).mean()
    title = "Z-scored mean minutely ask vol by hour"
    mean_minutely_ask_vol_by_hour.apply(scipy.stats.zscore).plot(title=title, legend=False)

# %%
bid_vol_df = bid_price_df.ffill().pct_change().rolling(rolling_window).std()
bid_vol_bps_df = 1e4 * bid_vol_df
hpandas.df_to_str(bid_vol_bps_df, log_level=logging.INFO)

# %%
bid_vol_bps_mean = bid_vol_bps_df.mean().sort_values()
bid_vol_bps_mean.name = "bid_vol_bps_mean"
#
if False:
    title = "bid vol"
    ylabel = "bps"
    bid_vol_bps_mean.plot(
        title=title,
        ylabel=ylabel,
        **config["plot_kwargs"],
    )

# %%
if False:
    mean_minutely_bid_vol_by_hour = bid_vol_bps_df.groupby(lambda x: x.hour).mean()
    title = "Z-scored mean minutely bid vol by hour"
    mean_minutely_bid_vol_by_hour.apply(scipy.stats.zscore).plot(title=title, legend=False)

# %%
total_vol_bps_df = np.sqrt(bid_vol_bps_df * ask_vol_bps_df)

# %%
total_vol_bps_mean = total_vol_bps_df.mean().sort_values()
total_vol_bps_mean.name = "total_vol_bps_mean"
#
title = "total vol"
ylabel = "bps"
total_vol_bps_mean.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)

# %%
mean_minutely_total_vol_by_hour = total_vol_bps_df.groupby(lambda x: x.hour).mean()
hpandas.df_to_str(mean_minutely_total_vol_by_hour, log_level=logging.INFO)

# %%
title = "Z-scored mean minutely total vol by hour"
mean_minutely_total_vol_by_hour.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="hour",
    ylabel="z-score",
)

# %%
mean_minutely_total_vol_by_minute = total_vol_bps_df.groupby(lambda x: x.minute).mean()
hpandas.df_to_str(mean_minutely_total_vol_by_minute, log_level=logging.INFO)

# %%
title = "Z-scored mean minutely total vol by_minute"
mean_minutely_total_vol_by_minute.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="minute",
    ylabel="z-score",
)

# %%
# Days of the week are numbered as follows:
# Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6.
mean_minutely_total_vol_by_weekday = total_vol_bps_df.groupby(total_vol_bps_df.index.weekday).mean()
hpandas.df_to_str(mean_minutely_total_vol_by_weekday.round(2), num_rows=7, log_level=logging.INFO)

# %%
title = "Z-scored mean minutely total vol by weekday"
mean_minutely_total_vol_by_weekday.apply(scipy.stats.zscore).plot(
    title=title,
    legend=False,
    xlabel="day of week (Monday = 0)",
    ylabel="z-score",
)

# %% [markdown]
# ## Vol to spread analysis

# %%
total_vol_to_half_spread = total_vol_bps_df.divide(half_spread_bps_df)
hpandas.df_to_str(total_vol_to_half_spread, log_level=logging.INFO)

# %%
total_vol_to_half_spread_mean = total_vol_to_half_spread.mean().sort_values(
    ascending=False
)
total_vol_to_half_spread_mean.name = "total_vol_to_half_spread_mean"
title = "Total minutely vol / half spread"
ax = total_vol_to_half_spread_mean.plot(
    title=title,
    **config["plot_kwargs"],
)
ax.invert_yaxis()

# %% [markdown]
# ## Top-of-book notional analysis

# %%
ask_notional_df = ask_price_df * ask_size_df
bid_notional_df = bid_price_df * bid_size_df
mean_notional_df = 0.5 * (ask_notional_df + bid_notional_df)

# %%
mean_notional_mean = mean_notional_df.mean().sort_values()
mean_notional_mean.name = "mean_notional_mean"
#
title = "mean notional"
ylabel = "dollars"
mean_notional_mean.plot(
    title=title,
    ylabel=ylabel,
    **config["plot_kwargs"],
)

# %%
# TODO: break this down by hour, minute, day-of-week.

# %% [markdown]
# # Compute rank correlation

# %%
liquidity_metrics_df = pd.concat(
    [
        mdv_notional,
        half_spread_bps_mean,
        total_vol_bps_mean,
        total_vol_to_half_spread_mean,
        mean_notional_mean,
    ],
    axis=1,
)
liquidity_metrics_df

# %%
liquidity_metrics_df.corr(method="spearman")

# %%
liquidity_metrics_df.corr(method="kendall")

# %% [markdown]
# # Partition universe

# %% [markdown]
# ## Get `total_vol_to_half_spread_mean` buckets

# %%
# Bucket sizes are arbitrary set and depend on time-frame, universe size.
# Consider adjusting sizes, when re-running.
#if config["partition_universe"]:
if True:
    # Get the vol metric values and put them in a DataFrame.
    total_vol_to_half_spread_mean_df = total_vol_to_half_spread_mean.sort_values(
        ascending=False
    ).to_frame()
    # Set buckets' cutpoints and labels.
    cutpoints = [-float("inf"), 1, 5, 10, 25, 250, float("inf")]
    labels = [
        "(-inf, 1]",
        "(1, 5]",
        "(5, 10]",
        "(10, 25]",
        "(25, 250]",
        "(250, inf)",
    ]
    # Assign buckets to full symbols.
    total_vol_to_half_spread_bucket = pd.cut(
        total_vol_to_half_spread_mean_df["total_vol_to_half_spread_mean"],
        bins=cutpoints,
        labels=labels,
    )
    total_vol_to_half_spread_bucket.name = "total_vol_to_half_spread_bucket"
    total_vol_to_half_spread_mean_df = pd.concat(
        [total_vol_to_half_spread_mean_df, total_vol_to_half_spread_bucket], axis=1
    )
    hpandas.df_to_str(total_vol_to_half_spread_mean_df, log_level=logging.INFO)

# %% [markdown]
# ## Get `half_spread_bps_mean` buckets

# %%
# Bucket sizes are arbitrary set and depend on time-frame, universe size.
# Consider adjusting sizes, when re-running.
#if config["partition_universe"]:
if True:    
    # Get the vol metric values and put them in a DataFrame.
    half_spread_bps_mean_df = half_spread_bps_mean.sort_values().to_frame()
    # Set buckets' cutpoints and labels.
    cutpoints = [-float("inf"), 0.05, 0.5, 1.0, 2.0, 10.0, float("inf")]
    labels = [
        "(-inf, 0.05]",
        "(0.05, 0.5]",
        "(0.5, 1.0]",
        "(1.0, 2.0]",
        "(2.0, 10.0]",
        "(10.0, inf)",
    ]
    # Assign buckets to full symbols.
    half_spread_bucket = pd.cut(
        half_spread_bps_mean_df["half_spread_bps_mean"],
        bins=cutpoints,
        labels=labels,
    )
    half_spread_bucket.name = "half_spread_bucket"
    half_spread_bps_mean_df = pd.concat(
        [half_spread_bps_mean_df, half_spread_bucket], axis=1
    )
    hpandas.df_to_str(half_spread_bps_mean_df, log_level=logging.INFO)

# %% [markdown]
# ## Partition by vol buckets

# %%
#if config["partition_universe"]:
if True:
    # Combine liquidity metrics and buckets in a single DataFrame.
    combined_liquidity_metrics_df = pd.concat(
        [liquidity_metrics_df, half_spread_bucket, total_vol_to_half_spread_bucket],
        axis=1,
    )
    # Get universe.
    v8_1_metrics_df = combined_liquidity_metrics_df[
        (
            combined_liquidity_metrics_df["half_spread_bucket"]
            == "(0.05, 0.5]"
        )
        & (
            combined_liquidity_metrics_df["total_vol_to_half_spread_bucket"]
            == "(25, 250]"
        )
    ]
    v8_1_universe = sorted(list(v8_1_metrics_df.index))
    print(v8_1_universe)

# %%
if True:
    v8_2_metrics_df = combined_liquidity_metrics_df[
        (
            combined_liquidity_metrics_df["half_spread_bucket"
            ].isin(["(0.05, 0.5]", "(0.5, 1.0]"])
        )
        & (
            combined_liquidity_metrics_df["total_vol_to_half_spread_bucket"
            ].isin(["(25, 250]", "(10, 25]"])
        )
        & (~combined_liquidity_metrics_df.index.isin(v8_1_universe))
    ]
    v8_2_universe = sorted(list(v8_2_metrics_df.index))
    print(v8_2_universe)

# %%
