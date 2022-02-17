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

import numpy as np
import pandas as pd

import core.pandas_helpers as cpanh
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im.common.data.types as imcodatyp
import im.kibot.data.load.kibot_s3_data_loader as imkdlksdlo
import im.kibot.metadata.load.s3_backend as imkmls3ba

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %%
# Disabling INFO messages from data downloads.
logger = logging.getLogger()
logger.setLevel(logging.CRITICAL)


# %% [markdown]
# # Functions

# %%
def calculate_datetime_statistics_for_kibot_data(
    list_of_symbols, contract_type, futures_frequency
):
    # Create dictionaries that will store the datetime statistics.
    start_date = {}
    end_date = {}
    data_count = {}
    # Create a loop that loads data for a single asset and proccess it to extract datetime statistics.
    for ticker in list_of_symbols:
        # The code below loads the data.
        if contract_type == "Futures":
            asset_df = kibot_loader.read_data(
                exchange="Any Exchange",
                symbol=ticker,
                asset_class=imcodatyp.AssetClass.Futures,
                contract_type=imcodatyp.ContractType.Continuous,
                frequency=futures_frequency,
            )
        elif contract_type == "Stocks":
            asset_df = kibot_loader.read_data(
                exchange="Any Exchange",
                symbol=ticker,
                asset_class=imcodatyp.AssetClass.Stocks,
                frequency=imcodatyp.Frequency.Minutely,
                unadjusted=False,
            )
        # Here is a condition that cuts out empty dataframes.
        if asset_df.shape[0] == 2:
            start_ind = {ticker: np.nan}
            start_date = start_date | start_ind.items()
            end_ind = {ticker: np.nan}
            end_date = end_date | end_ind.items()
            data_count_ind = {ticker: np.nan}
            data_count = data_count | data_count_ind.items()
        elif asset_df.shape[0] == 1:
            start_ind = {ticker: np.nan}
            start_date = start_date | start_ind.items()
            end_ind = {ticker: np.nan}
            end_date = end_date | end_ind.items()
            data_count_ind = {ticker: np.nan}
            data_count = data_count | data_count_ind.items()
        # The non-empty DataFrames are proccessed to extract datetime statistics.
        else:
            # Reseting index to unleash 'timestamp' column.
            asset_df.reset_index(inplace=True)
            # Collecting datetime statistics.
            max_date = asset_df["datetime"].max()
            min_date = asset_df["datetime"].min()
            data_points = asset_df["datetime"].count()
            # Writing these values into the dictionaries.
            start_ind = {ticker: min_date}
            start_date = start_date | start_ind.items()
            end_ind = {ticker: max_date}
            end_date = end_date | end_ind.items()
            data_count_ind = {ticker: data_points}
            data_count = data_count | data_count_ind.items()
        # Once all the dictionaries are filled with data - turn them to DataFrames.
        final_start_date = pd.DataFrame(
            start_date, columns=["", "start_date"]
        ).set_index("")
        final_end_date = pd.DataFrame(
            end_date, columns=["", "end_date"]
        ).set_index("")
        final_data_count = pd.DataFrame(
            data_count, columns=["", "data_points_count"]
        ).set_index("")
        # Combune all three statistics into one single DataFrame.
        result = pd.concat(
            [final_start_date, final_end_date, final_data_count], axis=1
        )
    return result.sort_index(ascending=True)


def calculate_general_datetime_stats(df):
    median_start_date = df["start_date"].median()
    median_end_date = df["end_date"].median()
    min_start_date = df["start_date"].min()
    max_end_date = df["end_date"].max()
    median_data_points = df["data_points_count"].median()
    result = pd.DataFrame(
        [
            median_start_date,
            median_end_date,
            min_start_date,
            max_end_date,
            median_data_points,
        ],
        index=[
            "median_start_date",
            "median_end_date",
            "min_start_date",
            "max_end_date",
            "median_data_points",
        ],
        columns=["value"],
    )
    return result


def raw_file_reader(path, s3_file, **kwargs):
    kwargs["s3fs"] = s3_file
    df = cpanh.read_csv(path, **kwargs)
    return df


def raw_file_reader_parquet(path, s3_file, **kwargs):
    kwargs["s3fs"] = s3_file
    df = cpanh.read_parquet(path, **kwargs)
    return df


# %% [markdown]
# # Explore the universe

# %%
s3_backend = imkmls3ba.S3Backend()

# %% [markdown]
# ## Futures

# %%
one_min_contract_metadata = s3_backend.read_1min_contract_metadata()
print("Number of contracts:", one_min_contract_metadata.shape[0])
display(one_min_contract_metadata.head(3))

# %%
daily_contract_metadata = s3_backend.read_daily_contract_metadata()
print("Number of contracts:", daily_contract_metadata.shape[0])
display(daily_contract_metadata.head(3))

# %%
tickbidask_contract_metadata = s3_backend.read_tickbidask_contract_metadata()
print("Number of contracts:", tickbidask_contract_metadata.shape[0])
display(tickbidask_contract_metadata.head(3))

# %% run_control={"marked": false}
continuous_contract_metadata = s3_backend.read_continuous_contract_metadata()
print("Number of contracts:", continuous_contract_metadata.shape[0])
display(continuous_contract_metadata.head(3))

# %%
kibot_exchange_mapping = s3_backend.read_kibot_exchange_mapping()
print("Number of contracts:", kibot_exchange_mapping.shape[0])
display(kibot_exchange_mapping.head(3))

# %% [markdown]
# ## Stocks

# %%
stocks_symbols = s3_backend.get_symbols_for_dataset("all_stocks_1min")
stocks_symbols[:5]

# %%
len(stocks_symbols)

# %% [markdown]
# # Example for data loading

# %%
kibot_loader = imkdlksdlo.KibotS3DataLoader()

# %% [markdown]
# ## Futures

# %%
# Example for CME Ethanol Daily Continuous Futures.
# Data is presented in OHLCV type.
ethanol_futures = kibot_loader.read_data(
    exchange="Unknown",
    symbol="AC",
    asset_class=imcodatyp.AssetClass.Futures,
    frequency=imcodatyp.Frequency.Daily,
    contract_type=imcodatyp.ContractType.Continuous,
)
ethanol_futures.head()

# %%
# Example for Minutely Expiry Futures (JAPANESE YEN JANUARY 2018).
japan_yen = kibot_loader.read_data(
    exchange="Unknown",
    symbol="JYF18",
    asset_class=imcodatyp.AssetClass.Futures,
    frequency=imcodatyp.Frequency.Minutely,
    contract_type=imcodatyp.ContractType.Expiry,
)
japan_yen.head()

# %% [markdown]
# ## Stocks

# %%
# Example for Apple stock.
aapl = kibot_loader.read_data(
    exchange="Q",
    symbol="AAPL",
    asset_class=imcodatyp.AssetClass.Stocks,
    frequency=imcodatyp.Frequency.Minutely,
    unadjusted=False,
)
aapl.head()

# %%
# Interesting note: the necessary param 'exchange' can be any value.
aapl_any_exchange = kibot_loader.read_data(
    exchange="Any Exchange",
    symbol="AAPL",
    asset_class=imcodatyp.AssetClass.Stocks,
    frequency=imcodatyp.Frequency.Minutely,
    unadjusted=False,
)
aapl_any_exchange.head()

# %% [markdown]
# # Period of time availability

# %% [markdown]
# ## Stocks

# %%
final_stats_stocks = calculate_datetime_statistics_for_kibot_data(
    stocks_symbols, "Stocks", "stock_datasets"
)
display(final_stats_stocks.shape)
display(final_stats_stocks)

# %%
general_stats_all_stocks = calculate_general_datetime_stats(final_stats_stocks)
general_stats_all_stocks

# %%
# DataFrame with empty stock data files.
empty_dataframes = final_stats_stocks[
    final_stats_stocks["data_points_count"].isna()
]
# Number of empty stock data files.
len(final_stats_stocks)

# %%
print(
    hprint.perc(len(empty_dataframes), len(final_stats)),
    "of files in stock universe are empty.",
)

# %% [markdown]
# ## Futures

# %% [markdown]
# ### Continuous contracts 1min

# %%
futures_continuous_contracts_1min_symbols = s3_backend.get_symbols_for_dataset(
    "all_futures_continuous_contracts_1min"
)
len(futures_continuous_contracts_1min_symbols)

# %%
# Getting a sample of 5 contracts.
futures_continuous_contracts_1min_symbols_sample = (
    futures_continuous_contracts_1min_symbols[:10]
)

# %%
continuous_contracts_minutely_stats = (
    calculate_datetime_statistics_for_kibot_data(
        futures_continuous_contracts_1min_symbols_sample,
        "Futures",
        imcodatyp.Frequency.Minutely,
    )
)
continuous_contracts_minutely_stats

# %% [markdown]
# ### Continuous contracts Daily

# %%
futures_continuous_contracts_daily_symbols = s3_backend.get_symbols_for_dataset(
    "all_futures_continuous_contracts_daily"
)
len(futures_continuous_contracts_daily_symbols)

# %%
continuous_contracts_daily_stats = calculate_datetime_statistics_for_kibot_data(
    futures_continuous_contracts_daily_symbols,
    "Futures",
    imcodatyp.Frequency.Daily,
)
continuous_contracts_daily_stats.head(3)

# %%
general_stats_all_futures = calculate_general_datetime_stats(
    continuous_contracts_daily_stats
)
general_stats_all_futures

# %% [markdown]
# # Read raw data

# %%
s3fs = hs3.get_s3fs("am")

# %% [markdown]
# ## Example of raw data for Stocks

# %%
file_path_stock = "s3://alphamatic-data/data/kibot/all_stocks_1min/AAPL.csv.gz"

# %%
aapl_raw = raw_file_reader(file_path_stock, s3fs)
aapl_raw.head()

# %% [markdown]
# ## Example of raw data for Futures

# %%
file_path_futures = "s3://alphamatic-data/data/kibot/all_futures_continuous_contracts_daily/AE.csv.gz"

# %%
ae_futures_raw = raw_file_reader(file_path_futures, s3fs)
ae_futures_raw.head()

# %% [markdown]
# ## Difference of raw Parquet stock data vs. CSV stock data

# %% [markdown]
# ### CSV example of QCOM

# %%
file_path_stock = "s3://alphamatic-data/data/kibot/all_stocks_1min/QCOM.csv.gz"

# %%
csv_qcom = raw_file_reader(file_path_stock, s3fs)
csv_qcom.head()

# %% [markdown]
# ### PQ example of QCOM

# %%
file_path_stock_parquet = (
    "s3://alphamatic-data/data/kibot/pq/all_stocks_1min/QCOM.pq"
)

# %%
pq_qcom = raw_file_reader_parquet(file_path_stock_parquet, s3fs)
pq_qcom.head()

# %% [markdown]
# # Summary

# %% [markdown]
# - The Kibot universe that is extracted using general methods is not consistent with the actual downloaded data and most likely is being parsed from the website at some period of time.
#    - In order to observe the actual universe that is available in the database one need to run get_symbols_for_dataset().
# - Data is presented in OHLCV format.
# - The necessary param 'exchange' from read_data() is not specific at all: it can be any value.
# - The stocks data in the database is huge and consists of >11.000 tickers.
#    - However, 17.62 % of files in stock universe consists no data.
# - The average available time period for stocks is ~5 years, up to 2020.
# - The futures availability in database is much less: 252 continuous contracts for both daily and minutely frequencies.
# - The OHLCV data inside raw files is identical by values and time range.
# - PQ data is already transformed to the desired format (unlike CSV data):
#    - The heading is in place.
#    - Datetime is converted to index and presented in a complete data-time format.
