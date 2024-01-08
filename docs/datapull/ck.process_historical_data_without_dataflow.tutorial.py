# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # ETL Tutorial For Historical Data

# %% [markdown]
# In this tutorial we are going to:
# 1. Extract a sample of historical trades data from `.tar` (source: https://drive.google.com/file/d/1up5otVlfw-RX1S6K8o4d2nNRPP-lKran/view)
# 2. Perform exploratory analysis 
# 3. Transform to a format supported by other components in the library.
# 4. Store on S3 in a parquet tiled format tailor-suited to be used for various use-cases.
# 5. Show an example of loading the data back from S3

# %% [markdown]
# # Prerequisites
#
# In order to go through this tutorial successfully, the following set-up/infrastructure available:
# 1. A virtual environment set-up running, make sure you can run `> i docker_jupyter` successfully.
# 2. An S3 bucket to store historical data
# 3. AWS API credentials set-up with permissions to access access the S3 bucket

# %% [markdown]
# # Decompress the input file

# %% [markdown]
# _Note: Assuming the tar archive is located in the root of the repository_

# %%
# ! mkdir data && tar xf /app/msfttaqcsv202308.tar -C ./data

# %%
# !ls ./data

# %% [markdown]
# # Imports

# %%
import datetime
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hparquet as hparque

# %% [markdown]
# The following cell sets up logging such that it is possible to capture log messages within jupyter cells

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Load the data
#
# Note: `head=10000` ensures we only use a snippet of the data to run a quick example.

# %%
data = pd.read_csv("data/uT1dPod8mR2s_MSFT US Equity_trades_1_1.csv.gz", head=10000)

# %%
data.head()

# %% [markdown]
# Drop columns where only NaN values are present.

# %%
data = data.dropna(axis=1, how='all')

# %%
data["SECURITY"].value_counts()

# %% [markdown]
# Set datetime index

# %%
data["timestamp"] = pd.to_datetime(data["TRADE_REPORTED_TIME"])
data = data.set_index("timestamp", drop=True)

# %%
data = data[["EVT_TRADE_PRICE", "EVT_TRADE_SIZE"]]

# %% [markdown]
# ## Compute OHLCV
#
#
# A simple resampling operation is applied to the data
#
#
# Time interval labelling convention used across that time interval [a, b) is labelled as b.
#
# E.g. for interval [06:40:00, 06:41:00) the timestamp is
# 06:41:00
#
# Reference: [Sorrentum whitepaper](https://drive.google.com/drive/u/0/folders/1oFRoJIpqsbCJGP54vx774eVOBCc0z6Wk)

# %%
data_ohlcv = data["EVT_TRADE_PRICE"].resample("1T", closed="left", label="right").ohlc()

# %%
data_volume = data["EVT_TRADE_SIZE"].resample("1T", closed="left", label="right").sum()
data_volume.name = "volume"

# %%
data = pd.concat([data_ohlcv, data_volume], axis=1)

# %%
data.head()

# %% [markdown]
# A standardized name for asset identification column is currently `currency_pair`

# %%
data["currency_pair"] = "MSFT"
data["knowledge_timestamp"] = pd.Timestamp.utcnow()

# %%
data.head()

# %% [markdown]
# ## Save as parquet to S3

# %% [markdown]
# - The S3 path is formed based on a dataset schema we use. It allows use to have a predictable, unified structure. See Sorrentum whitepaper and `dataset_schema/` directory

# %%
# See docstring of hparque.add_date_partition_columns and hparque.to_partitioned_parquet.
partition_mode = "by_year_month"
s3_path = "s3://cryptokaizen-data-test/v3/bulk/manual/resampled_1min/parquet/ohlcv/spot/v1/bloomberg/us_market/v1_0_0/"
# The value of aws_profile depends on your organization set-up.
aws_profile = "ck"

# %%
data, partition_cols = hparque.add_date_partition_columns(
        data, partition_mode
    )
hparque.to_partitioned_parquet(
    data,
    ["currency_pair"] + partition_cols,
    s3_path,
    partition_filename=None,
    aws_profile=aws_profile,
)

# %% [markdown]
# # Demonstration of example usage of stored resampled data
#
# Once the data is stored to S3 it can be used for any vairous use-cases. An example is using `HistoricalPqByCurrencyPairTileClient` to load data back from S3 and create a `MarketData` object which can be used for example in simulations or backtests.

# %% [markdown]
# ## Add additional import for the use case

# %%
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
import market_data as mdata
import core.config as cconfig

# %% [markdown]
# ## Build Config
#
# Config is the standardized way of setting parameters. See more in the documentation

# %%
config = {
    "start_ts": None,
    "end_ts": None,
    "wall_clock_time": pd.Timestamp("2100-01-01T00:00:00+00:00"),
    "columns": None,
    "columns_remap": None,
    "ts_col_name": "end_ts",
    "im_client": {
        "vendor": "bloomberg",
        "universe_version": "v1",
        "root_dir": "s3://cryptokaizen-data-test/v3/bulk",
        "partition_mode": "by_year_month",
        "dataset": "ohlcv",
        "contract_type": "spot",
        "data_snapshot": "",
        "download_mode": "manual",
        "downloading_entity": "",
        "aws_profile": "ck",
        "resample_1min": False,
        "version": "v1_0_0",
        "tag": "resampled_1min",
    },
}
config = cconfig.Config.from_dict(config)
print(config)

# %% [markdown]
# ## Load data

# %% [markdown]
# Client provides an interface to load data from storage medium. More on clients in the dedicated documentation

# %%
im_client = imvcdchpcl.HistoricalPqByCurrencyPairTileClient(**config["im_client"])

# %% [markdown]
# To represent a set of assets which are used for a specific use case (for example set of assets traded with a given model) we use a universe. To find out more, search for "universe" documentation.

# %%
full_symbols = im_client.get_universe()
filter_data_mode = "assert"
actual_df = im_client.read_data(
    full_symbols,
    config["start_ts"],
    config["end_ts"],
    config["columns"],
    filter_data_mode,
)
hpandas.df_to_str(actual_df, num_rows=5, log_level=logging.INFO)

# %% [markdown]
# ### Initialize MarketData
#
# #TODO(Juraj): The problem I see here is that we have these cryptically sounding powerful functions such as `get_HistoricalImClientMarketData_example1` but it's difficult for a newcomer/client to understand what to do in case the use-case or input arguments are a little bit different

# %%
asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
market_data = mdata.get_HistoricalImClientMarketData_example1(
    im_client,
    asset_ids,
    config["columns"],
    config["columns_remap"],
    wall_clock_time=config["wall_clock_time"],
)

# %%
asset_ids = None
market_data_df = market_data.get_data_for_interval(
    config["start_ts"], config["end_ts"], config["ts_col_name"], asset_ids
)
hpandas.df_to_str(market_data_df, num_rows=5, log_level=logging.INFO)

# %%
