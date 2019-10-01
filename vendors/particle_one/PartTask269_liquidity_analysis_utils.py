import logging

import pandas as pd

from vendors.kibot import utils as kut

_LOG = logging.getLogger(__name__)


def get_daily_prices(symbols, n_rows):
    file_name = "/data/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
    daily_price_dict_df = kut.read_multiple_symbol_data(symbols, file_name,
                                                        nrows=n_rows)
    return daily_price_dict_df


def get_sum_daily_volume(daily_price_dict_df):
    daily_volume_sum_dict = {symbol: daily_prices_symbol["vol"].sum() for
                             symbol, daily_prices_symbol in
                             daily_price_dict_df.items()}
    daily_volume_sum_df = pd.DataFrame.from_dict(daily_volume_sum_dict,
                                                 orient="index",
                                                 columns=["sum_vol"])
    daily_volume_sum_df.index.name = "symbol"
    return daily_volume_sum_df


def get_mean_daily_volume(daily_price_dict_df):
    daily_volume_mean_dict = {symbol: daily_prices_symbol["vol"].mean() for
                              symbol, daily_prices_symbol in
                              daily_price_dict_df.items()}
    daily_volume_mean_df = pd.DataFrame.from_dict(daily_volume_mean_dict,
                                                  orient="index",
                                                  columns=["mean_vol"])
    daily_volume_mean_df.index.name = "symbol"
    return daily_volume_mean_df
