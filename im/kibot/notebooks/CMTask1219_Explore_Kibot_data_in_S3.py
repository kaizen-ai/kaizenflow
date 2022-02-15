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
# Getting the sample of 20 stocks, as the total number of stocks is >11K.
stocks_symbols_sample = stocks_symbols[:20]

# %%
# The tickers below has no data.
# stocks_symbols_sample.remove("AACC")
# stocks_symbols_sample.remove("AACOU")
# stocks_symbols_sample.remove("AACOW")
# stocks_symbols_sample.remove("AAI")
# stocks_symbols_sample.remove("AAMRQ")

# %% run_control={"marked": false}
# %%time
result = []

for ticker in stocks_symbols_sample:
    stock_df = kibot_loader.read_data(
        exchange="Any Exchange",
        symbol=ticker,
        asset_class=imcodatyp.AssetClass.Stocks,
        frequency=imcodatyp.Frequency.Minutely,
        unadjusted=False,
    )
    if stock_df.iloc[0][0] == "405 Data Not Found.":
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
result = pd.concat(result)

# %%
result

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
result = pd.concat(result)

# %%
result

# %%
result["start_date"].min()

# %%
result["end_date"].max()
