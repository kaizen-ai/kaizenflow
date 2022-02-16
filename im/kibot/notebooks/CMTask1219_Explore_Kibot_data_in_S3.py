# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Imports

# %%
import numpy as np
import pandas as pd

import im.common.data.types as imcodatyp
import im.kibot.data.load.kibot_s3_data_loader as imkdlksdlo
import im.kibot.metadata.load.s3_backend as imkmls3ba

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
# example for CME Ethanol Daily Continuous Futures.
# Data is presented in OHLCV type.
kibot_loader.read_data(
    exchange="Unknown",
    symbol="AC",
    asset_class=imcodatyp.AssetClass.Futures,
    frequency=imcodatyp.Frequency.Daily,
    contract_type=imcodatyp.ContractType.Continuous,
)

# %%
# example for Minutely Expiry Futures (JAPANESE YEN JANUARY 2018)
kibot_loader.read_data(
    exchange="Unknown",
    symbol="JYF18",
    asset_class=imcodatyp.AssetClass.Futures,
    frequency=imcodatyp.Frequency.Minutely,
    contract_type=imcodatyp.ContractType.Expiry,
)

# %% [markdown]
# ## Stocks

# %%
# example for Apple stock.
kibot_loader.read_data(
    exchange="Q",
    symbol="AAPL",
    asset_class=imcodatyp.AssetClass.Stocks,
    frequency=imcodatyp.Frequency.Minutely,
    unadjusted=False,
)

# %%
# Interesting note: the necessary param 'exchange' can be any value.
kibot_loader.read_data(
    exchange="Any Exchange",
    symbol="AAPL",
    asset_class=imcodatyp.AssetClass.Stocks,
    frequency=imcodatyp.Frequency.Minutely,
    unadjusted=False,
)

# %% [markdown]
# # Period of time availability

# %% [markdown]
# ## Stocks

# %%
# %%time
result = []

for ticker in stocks_symbols:
    stock_df = kibot_loader.read_data(
        exchange="Any Exchange",
        symbol=ticker,
        asset_class=imcodatyp.AssetClass.Stocks,
        frequency=imcodatyp.Frequency.Minutely,
        unadjusted=False,
    )
    if stock_df.shape[0] == 2:
        datetime_stats = pd.DataFrame()
        datetime_stats.index = [ticker]
        datetime_stats["start_date"] = np.nan
        datetime_stats["end_date"] = np.nan
        datetime_stats["data_points_count"] = np.nan
        # datetime_stats["number_of_nans"] = len(stock_df[stock_df["close"].isna()])
        result.append(datetime_stats)
    elif stock_df.shape[0] == 1:
        datetime_stats = pd.DataFrame()
        datetime_stats.index = [ticker]
        datetime_stats["start_date"] = np.nan
        datetime_stats["end_date"] = np.nan
        datetime_stats["data_points_count"] = np.nan
        # datetime_stats["number_of_nans"] = len(stock_df[stock_df["close"].isna()])
        result.append(datetime_stats)
    else:
        # Reseting index to unleash 'timestamp' column.
        stock_df.reset_index(inplace=True)
        # Start-end date.
        max_date = pd.Series(
            stock_df["datetime"].describe(datetime_is_numeric=True).loc["max"]
        )
        min_date = pd.Series(
            stock_df["datetime"].describe(datetime_is_numeric=True).loc["min"]
        )
        # Number of timestamps for each coin.
        data_points = pd.Series(
            stock_df["datetime"].describe(datetime_is_numeric=True).loc["count"]
        )
        # Attach calculations to the DataFrame.
        datetime_stats = pd.DataFrame()
        datetime_stats["start_date"] = min_date
        datetime_stats["end_date"] = max_date
        datetime_stats["data_points_count"] = data_points
        # datetime_stats["number_of_nans"] = len(stock_df[stock_df["close"].isna()])
        datetime_stats.index = [ticker]
        result.append(datetime_stats)
all_stocks_stats = pd.concat(result)

# %%
#final_stats = all_stocks_stats.copy()

# %%
display(final_stats.shape)
display(final_stats)

# %%
general_stats_all_stocks = pd.DataFrame()
general_stats_all_stocks.loc["median_start_date","value"] = final_stats["start_date"].median()
general_stats_all_stocks.loc["median_end_date","value"] = final_stats["end_date"].median()
general_stats_all_stocks.loc["min_start_date","value"] = final_stats["start_date"].min()
general_stats_all_stocks.loc["max_end_date","value"] = final_stats["end_date"].max()
general_stats_all_stocks.loc["median_data_points","value"] = final_stats["data_points_count"].median()
general_stats_all_stocks

# %%
# DataFrame with empty stock data files.
empty_dataframes = final_stats[final_stats["data_points_count"].isna()]
# Number of empty stock data files.
len(empty_dataframes)

# %%
print(round(100*len(empty_dataframes)/len(final_stats),2),"% of files in stock universe are empty.")

# %%
# Tickers with empty dataframes.
for ticker in list(empty_dataframes.index): print(ticker)

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
# Getting a sample of 10 contracts.
futures_continuous_contracts_1min_symbols_sample = (
    futures_continuous_contracts_1min_symbols[:10]
)

# %%
# %%time
result = []

for ticker in futures_continuous_contracts_1min_symbols_sample:
    futures_df = kibot_loader.read_data(
        exchange="Any Exchange",
        symbol=ticker,
        asset_class=imcodatyp.AssetClass.Futures,
        contract_type=imcodatyp.ContractType.Continuous,
        frequency=imcodatyp.Frequency.Minutely,
    )
    if futures_df.iloc[0][0] == "405 Data Not Found.":
        datetime_stats = pd.DataFrame()
        datetime_stats.index = [ticker]
        datetime_stats["start_date"] = np.nan
        datetime_stats["end_date"] = np.nan
        datetime_stats["data_points_count"] = np.nan
        # datetime_stats["number_of_nans"] = len(futures_df[futures_df["close"].isna()])
        result.append(datetime_stats)
    else:
        # Reseting index to unleash 'timestamp' column.
        futures_df.reset_index(inplace=True)
        # Start-end date.
        max_date = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["max"]
        )
        min_date = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["min"]
        )
        # Number of timestamps for each coin.
        data_points = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["count"]
        )
        # Attach calculations to the DataFrame.
        datetime_stats = pd.DataFrame()
        datetime_stats["start_date"] = min_date
        datetime_stats["end_date"] = max_date
        datetime_stats["data_points_count"] = data_points
        # datetime_stats["number_of_nans"] = len(futures_df[futures_df["close"].isna()])
        datetime_stats.index = [ticker]
        result.append(datetime_stats)
result = pd.concat(result)

# %%
result

# %% [markdown]
# ### Continuous contracts Daily

# %%
futures_continuous_contracts_daily_symbols = s3_backend.get_symbols_for_dataset(
    "all_futures_continuous_contracts_daily"
)
len(futures_continuous_contracts_daily_symbols)

# %%
# %%time
result = []

for ticker in futures_continuous_contracts_daily_symbols:
    futures_df = kibot_loader.read_data(
        exchange="Any Exchange",
        symbol=ticker,
        asset_class=imcodatyp.AssetClass.Futures,
        contract_type=imcodatyp.ContractType.Continuous,
        frequency=imcodatyp.Frequency.Daily,
    )
    if futures_df.iloc[0][0] == "405 Data Not Found.":
        datetime_stats = pd.DataFrame()
        datetime_stats.index = [ticker]
        datetime_stats["start_date"] = np.nan
        datetime_stats["end_date"] = np.nan
        datetime_stats["data_points_count"] = np.nan
        # datetime_stats["number_of_nans"] = len(futures_df[futures_df["close"].isna()])
        result.append(datetime_stats)
    else:
        # Reseting index to unleash 'timestamp' column.
        futures_df.reset_index(inplace=True)
        # Start-end date.
        max_date = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["max"]
        )
        min_date = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["min"]
        )
        # Number of timestamps for each coin.
        data_points = pd.Series(
            futures_df["datetime"].describe(datetime_is_numeric=True).loc["count"]
        )
        # Attach calculations to the DataFrame.
        datetime_stats = pd.DataFrame()
        datetime_stats["start_date"] = min_date
        datetime_stats["end_date"] = max_date
        datetime_stats["data_points_count"] = data_points
        # datetime_stats["number_of_nans"] = len(futures_df[futures_df["close"].isna()])
        datetime_stats.index = [ticker]
        result.append(datetime_stats)
continuous_contracts_daily_stats = pd.concat(result)

# %%
continuous_contracts_daily_stats

# %%
general_stats = pd.DataFrame()
general_stats.loc["median_start_date","value"] = continuous_contracts_daily_stats["start_date"].median()
general_stats.loc["median_end_date","value"] = continuous_contracts_daily_stats["end_date"].median()
general_stats.loc["min_start_date","value"] = continuous_contracts_daily_stats["start_date"].min()
general_stats.loc["max_end_date","value"] = continuous_contracts_daily_stats["end_date"].max()
general_stats.loc["median_data_points","value"] = continuous_contracts_daily_stats["data_points_count"].median()
general_stats

# %% [markdown]
# # Read raw data

# %%
import helpers.hs3 as hs3
import core.pandas_helpers as cpanh


# %%
def raw_file_reader(path, s3_file, **kwargs):
    kwargs["s3fs"] = s3_file
    df = cpanh.read_csv(path, **kwargs)
    return df


# %%
s3fs = hs3.get_s3fs("am")

# %% [markdown]
# ## Example of raw data for Stocks

# %%
file_path_stock = "s3://alphamatic-data/data/kibot/all_stocks_1min/AAPL.csv.gz"
hs3.is_s3_path(file_path_stock)

# %%
file_reader(file_path_stock, s3fs)

# %% [markdown]
# ## Example of raw data for Futures

# %%
file_path_futures = "s3://alphamatic-data/data/kibot/all_futures_continuous_contracts_daily/AE.csv.gz"
hs3.is_s3_path(file_path_futures)

# %%
file_reader(file_path_futures, s3fs)

# %% [markdown]
# ## Difference of raw Parquet stock data vs. CSV stock data

# %% [markdown]
# ### CSV example of QCOM

# %%
file_path_stock = "s3://alphamatic-data/data/kibot/all_stocks_1min/QCOM.csv.gz"
hs3.is_s3_path(file_path_stock)

# %%
raw_file_reader(file_path_stock, s3fs)


# %% [markdown]
# ### PQ example of QCOM

# %%
def raw_file_reader_parquet(path, s3_file, **kwargs):
    kwargs["s3fs"] = s3_file
    df = cpanh.read_parquet(path, **kwargs)
    return df


# %%
file_path_stock_parquet = "s3://alphamatic-data/data/kibot/pq/all_stocks_1min/QCOM.pq"
hs3.is_s3_path(file_path_stock_parquet)

# %%
raw_file_reader_parquet(file_path_stock_parquet, s3fs)

# %% [markdown]
# ### Summary

# %% [markdown]
# - The OHLCV data inside those files are identical (by values and time range)
# - PQ data is already transformed to the desired format:
#    - The heading is in place
#    - Datetime is converted to index and presented in a complete data-time format

# %%
