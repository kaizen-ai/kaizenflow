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
# The notebooks performs comparison between bid-ask data loaded from `"v1_0_0"`and `"v2_0_0"` data versions.

# %% [markdown]
# # Imports

# %%
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline

# %%
import logging

import pandas as pd

import core.config as cconfig
import core.finance as cofinanc
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.common.universe as ivcu
import market_data.market_data_example as mdmadaex

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
universe_version = "v8.1"
wall_clock_time = pd.Timestamp("2100-01-01T00:00:00+00:00")
config = {
    "universe": {
        "vendor": "CCXT",
        "mode": "trade",
        "version": universe_version,
        "as_full_symbol": True,
    },
    "US_equities_tz": "America/New_York",
    "bid_ask_data": {
        "start_timestamp": pd.Timestamp("2024-01-23T00:00:00+00:00"),
        "end_timestamp": pd.Timestamp("2024-02-01T23:59:00+00:00"),
        "column_names": {
            "timestamp": "timestamp",
        },
        "im_client_config_v1": {
            "universe_version": "v8",
            "root_dir": "s3://cryptokaizen-data-test/v3",
            "partition_mode": "by_year_month",
            "dataset": "bid_ask",
            "contract_type": "futures",
            "data_snapshot": "",
            "version": "v1_0_0",
            "download_universe_version": "v8",
            "tag": "resampled_1min",
            "aws_profile": "ck",
        },
        "market_data_config_v1": {
            "columns": [
                "level_1.bid_price.open",
                "level_1.bid_price.high",
                "level_1.bid_price.low",
                "level_1.bid_price.close",
                "level_1.bid_price.mean",
                "level_1.bid_size.open",
                "level_1.bid_size.max",
                "level_1.bid_size.min",
                "level_1.bid_size.close",
                "level_1.bid_size.mean",
                "level_1.ask_price.open",
                "level_1.ask_price.high",
                "level_1.ask_price.low",
                "level_1.ask_price.close",
                "level_1.ask_price.mean",
                "level_1.ask_size.open",
                "level_1.ask_size.max",
                "level_1.ask_size.min",
                "level_1.ask_size.close",
                "level_1.ask_size.mean",
                "asset_id",
                "full_symbol",
                "start_ts",
                "knowledge_timestamp",
            ],
            "column_remap": None,
            "wall_clock_time": wall_clock_time,
            "filter_data_mode": "assert",
        },
        "im_client_config_v2": {
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
        "market_data_config_v2": {
            "columns": cofinanc.get_bid_ask_columns_by_level(1)
            + ["asset_id", "full_symbol", "start_ts", "knowledge_timestamp"],
            "column_remap": None,
            "wall_clock_time": wall_clock_time,
            "filter_data_mode": "assert",
        },
        "column_names": {
            "timestamp": "timestamp",
            "full_symbol": "full_symbol",
            "close": "close",
            "volume": "volume",
            "volume_notional": "volume_notional",
            "ask_price": "level_1.ask_price.close",
            "bid_price": "level_1.bid_price.close",
        },
    },
}
config = cconfig.Config().from_dict(config)
print(config)

# %% [markdown]
# # Universe

# %%
# Get full symbol universe.
full_symbols = ivcu.get_vendor_universe(**config["universe"])
_LOG.info("The number of coins in the universe=%s", len(full_symbols))
full_symbols

# %%
# Get asset ids.
asset_ids = [
    ivcu.string_to_numerical_id(full_symbol) for full_symbol in full_symbols
]
asset_ids

# %% [markdown]
# # Bid / ask price changes

# %% [markdown]
# ## `"v1_0_0"`

# %%
bid_ask_im_client_v1 = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    **config["bid_ask_data"]["im_client_config_v1"]
)

# %%
bid_ask_market_data_v1 = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client_v1,
    asset_ids,
    **config["bid_ask_data"]["market_data_config_v1"],
)

# %%
bid_ask_data_v1 = bid_ask_market_data_v1.get_data_for_interval(
    config["bid_ask_data"]["start_timestamp"],
    config["bid_ask_data"]["end_timestamp"],
    config["bid_ask_data"]["column_names"]["timestamp"],
    asset_ids,
)
# Convert to ET to be able to compare with US equities active trading hours.
bid_ask_data_v1.index = bid_ask_data_v1.index.tz_convert(config["US_equities_tz"])
hpandas.df_to_str(bid_ask_data_v1, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## `"v2_0_0"`

# %%
bid_ask_im_client_v2 = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
    **config["bid_ask_data"]["im_client_config_v2"]
)

# %%
bid_ask_market_data_v2 = mdmadaex.get_HistoricalImClientMarketData_example1(
    bid_ask_im_client_v2,
    asset_ids,
    **config["bid_ask_data"]["market_data_config_v2"],
)

# %%
bid_ask_data_v2 = bid_ask_market_data_v2.get_data_for_interval(
    config["bid_ask_data"]["start_timestamp"],
    config["bid_ask_data"]["end_timestamp"],
    config["bid_ask_data"]["column_names"]["timestamp"],
    asset_ids,
)
# Convert to ET to be able to compare with US equities active trading hours.
bid_ask_data_v2.index = bid_ask_data_v2.index.tz_convert(config["US_equities_tz"])
hpandas.df_to_str(bid_ask_data_v2, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ## Compare data

# %%
cols_to_drop = ["full_symbol", "knowledge_timestamp", "start_ts"]

# %%
bid_ask_data_v1_0_0 = bid_ask_data_v1.drop(cols_to_drop, axis=1).pivot(
    columns="asset_id"
)
bid_ask_data_v2_0_0 = bid_ask_data_v2.drop(cols_to_drop, axis=1).pivot(
    columns="asset_id"
)

# %%
diff_df_pct = hpandas.compare_dfs(
    bid_ask_data_v1_0_0,
    bid_ask_data_v2_0_0,
    row_mode="inner",
    column_mode="inner",
    diff_mode="pct_change",
    assert_diff_threshold=0.0001,
)
hpandas.df_to_str(diff_df_pct, num_rows=5, log_level=logging.INFO)

# %%
diff_stats_per_column = (
    100
    * diff_df_pct[abs(diff_df_pct) > 0.01].count().groupby(level=[0]).mean()
    / len(diff_df_pct)
)
display(diff_stats_per_column)
diff_stats_per_column.plot(kind="bar")

# %%
diff_stats_per_asset = (
    100
    * diff_df_pct[abs(diff_df_pct) > 0.01].count().groupby(level=[1]).mean()
    / len(diff_df_pct)
)
display(diff_stats_per_asset)
diff_stats_per_asset.plot(kind="bar")

# %%
