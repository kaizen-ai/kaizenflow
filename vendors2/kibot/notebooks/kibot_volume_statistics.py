# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
#
# # Imports
#
# Importing all required modules.

# %% pycharm={"name": "#%%\n"}
# %load_ext autoreload
# %autoreload 2

import datetime as dt
import os

import pandas as pd

import helpers.s3 as hs3
import vendors2.kibot.data.load as kdl
import vendors2.kibot.data.load.file_path_generator as fpgen
import vendors2.kibot.data.types as types

# This will be changed later when Exchange will be developed.
EXCHANGE = "TestExchange"

# %% [markdown] pycharm={"name": "#%% md\n"}
# Define helper functions to calculate the report.


# %% pycharm={"name": "#%%\n"}
def slice_price_data(
    prices: pd.DataFrame, last_years: int
) -> pd.DataFrame:
    """Slice DataFrames for each symbol to contain records only for the
    last_years years.

    :param prices: price for symbol dataframe.
    :param last_years: Number of years data is averaged to.
    :return: dataframe sliced.
    """
    now = dt.datetime.now()
    # TODO(vr): check if dateutils.relativedate is better?
    before = now - dt.timedelta(days=last_years * 365)
    return prices.loc[before:now]


def get_start_date(prices_df: pd.DataFrame) -> str:
    """Extract start dates for each time series.

    :param prices_df: dataframe with prices
    :return: start date as string
    """
    start_date = prices_df.index[0].strftime("%Y-%m-%d")
    return start_date


def get_price_data(
    prices_df: pd.DataFrame,
    price_col: str,
    agg_func: str,
) -> float:
    """Get grouped prices for each symbol.

    :param prices_df: dataframe with prices
    :param price_col: The name of the price column
    :param agg_func: The name of the aggregation function that needs to
        be applied to the prices for each symbol
    :return:
    """
    val = getattr(prices_df[price_col], agg_func)()
    return val


# %% [markdown] pycharm={"name": "#%% md\n"}
# Define main method to generate the report for a dataset.

# %% pycharm={"name": "#%%\n"}
def generate_report(
    exchange: str,
    asset_class: types.AssetClass,
    contract_type: types.ContractType,
    frequency: types.Frequency
) -> pd.DataFrame:
    """Generate a report for a dataset.

    :param exchange:
    :param frequency: `D` or `T` for daily or minutely data respectively
    :param asset_class:
    :param contract_type: `continuous` or `expiry`
    :return: a dataframe with the report
    """
    dataset_aws_path = fpgen.FilePathGenerator().generate_file_path( "",
        frequency, asset_class, contract_type, ext=types.Extension.CSV
    )
    dataset_aws_directory = os.path.dirname(dataset_aws_path)
    # Get a list of payloads (symbols) in format XYZ.csv.gz.
    payloads = hs3.listdir(dataset_aws_directory, mode="non-recursive")
    # Get only first n-rows.
    n_rows = 10
    # Get only symbols list.
    symbols = tuple(
        payload.replace(".csv.gz", "") for payload in payloads[:n_rows]
    )
    # Read dataframes.
    kibot_data_loader = kdl.S3KibotDataLoader()
    report_data = []
    for symbol in symbols:
        df = kibot_data_loader.read_data(exchange, symbol, asset_class, frequency, contract_type)
        # Get avg. vol for the last 1 year
        price_1y_df = slice_price_data(df, last_years=1)
        mean_1y_vol = get_price_data(price_1y_df, "vol", "mean")
        # Get avg. vol for the last 3 years
        price_3y_df = slice_price_data(df, last_years=3)
        mean_3y_vol = get_price_data(price_3y_df, "vol", "mean")
        # Get avg. vol for the last 5 years
        price_5y_df = slice_price_data(df, last_years=5)
        mean_5y_vol = get_price_data(price_5y_df, "vol", "mean")
        # Get start date for each symbol.
        start_date = get_start_date(df)
        report_data.append((symbol, start_date, mean_1y_vol, mean_3y_vol, mean_5y_vol))
    report = pd.DataFrame.from_records(
        report_data,
        index="symbol",
        columns=["symbol", "start_date", "mean_1y_vol", "mean_3y_vol", "mean_5y_vol"]
    )
    report.fillna(0, inplace=True)
    return report


# %% [markdown] pycharm={"name": "#%% md\n"}
# Report for all_futures_contracts_1min

# %% pycharm={"name": "#%%\n"}
dataset_report = generate_report(
    EXCHANGE,
    types.AssetClass.Futures,
    types.ContractType.Expiry,
    types.Frequency.Minutely
)
dataset_report

# %% [markdown]
# Report for all_futures_contracts_daily

# %% pycharm={"name": "#%%\n"}
dataset_report = generate_report(
    EXCHANGE,
    types.AssetClass.Futures,
    types.ContractType.Expiry,
    types.Frequency.Daily
)
dataset_report

# %% [markdown]
# Report for all_futures_continuous_contracts_1min

# %% pycharm={"name": "#%%\n"}
dataset_report = generate_report(
    EXCHANGE,
    types.AssetClass.Futures,
    types.ContractType.Continuous,
    types.Frequency.Minutely
)
dataset_report

# %% [markdown]
# Report for all_futures_continuous_contracts_daily

# %% pycharm={"name": "#%%\n"}
dataset_report = generate_report(
    EXCHANGE,
    types.AssetClass.Futures,
    types.ContractType.Continuous,
    types.Frequency.Daily
)
dataset_report
