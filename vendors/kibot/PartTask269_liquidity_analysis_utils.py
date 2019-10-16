import functools
import logging
import os
from typing import Callable, Dict, Optional

import matplotlib.pyplot as plt
import pandas as pd

import helpers.dbg as dbg
import helpers.s3 as hs3
import vendors.cme.read as cmer
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)

KIBOT_VOL = "vol"


def get_sum_prices(
    price_df_dict: Dict[str, pd.DataFrame], price_col: str
) -> pd.DataFrame:
    """
    Get sum of the prices for each symbol.

    :param price_df_dict: {symbol: prices_for_symbol_df}
    :param price_col: The name of the price column
    :return: pd.DataFrame indexed by symbol
    """
    prices_sum_df = _get_prices(price_df_dict, price_col, "sum")
    return prices_sum_df


def get_mean_prices(
    price_df_dict: Dict[str, pd.DataFrame], price_col: str
) -> pd.DataFrame:
    """
    Get mean of the prices for each symbol.

    :param price_df_dict: {symbol: prices_for_symbol_df}
    :param price_col: The name of the price column
    :return: pd.DataFrame indexed by symbol
    """
    prices_mean_df = _get_prices(price_df_dict, price_col, "mean")
    return prices_mean_df


def get_kibot_reader(
    frequency: str, symbol: str, n_rows: Optional[int]
) -> Callable:
    dbg.dassert_in(
        frequency,
        ["D", "M"],
        "Only daily ('D') and minutely ('M') frequencies are supported.",
    )
    if frequency == "M":
        dir_path = os.path.join(
            hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_1min"
        )
    else:
        dir_path = os.path.join(
            hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_daily"
        )
    file_name = os.path.join(dir_path, f"{symbol}.csv.gz")
    reader = functools.partial(kut.read_data, file_name, nrows=n_rows)
    return reader


def read_kibot_prices(
    frequency: str, symbol: str, n_rows: Optional[int]
) -> pd.DataFrame:
    reader = get_kibot_reader(frequency, symbol, n_rows)
    prices = reader()
    return prices


def _get_prices(
    price_df_dict: Dict[str, pd.DataFrame], price_col: str, agg_func: str
) -> pd.DataFrame:
    """
    Get grouped prices for each symbol.

    :param price_df_dict: {symbol: prices_for_symbol_df}
    :param price_col: The name of the price column
    :param agg_func: The name of the aggregation function that needs to
        be applied to the prices for each symbol
    :return: pd.DataFrame indexed by symbol
    """
    price_dict = {
        symbol: getattr(prices[price_col], agg_func)()
        for symbol, prices in price_df_dict.items()
    }
    price_df = pd.DataFrame.from_dict(
        price_dict, orient="index", columns=[f"{agg_func}_{price_col}"]
    )
    price_df.index.name = "symbol"
    return price_df


class PricesStudy:
    """
    Perform a basic study of daily and minutely prices.

    - Read daily and minutely prices
    - Plot daily and minutely prices for column
        - by year
        - by month
        - by day of week
        - by hour
    """

    def __init__(
        self,
        data_reader: Callable[[str, str, Optional[int]], pd.DataFrame],
        symbol: str,
        price_col: str,
        n_rows: Optional[int],
    ):
        """
        :param data_reader: A function that takes frequency
            (daily/minutely), symbol and n_rows as input parameters
            and returns a prices dataframe.
        :param symbol: The symbol for which the price needs to be
            studied
        :param price_col: The name of the price column
        :param n_rows: the maximum numer of rows to load
        """
        self._symbol = symbol
        self._nrows = n_rows
        self._data_reader = data_reader
        self.daily_prices = self._data_reader("daily", self._symbol, self._nrows)
        self.minutely_prices = self._data_reader(
            "minutely", self._symbol, self._nrows
        )
        self._price_col = price_col

    def execute(self):
        self.plot_prices("daily")
        self.plot_price_changes_by_year("daily")
        self.plot_mean_price_day_of_month("daily")
        self.plot_mean_price_day_of_week("daily")
        #
        self.plot_prices("minutely")
        self.plot_price_changes_by_year("minutely")
        self.plot_mean_price_day_of_week("minutely")
        self.plot_minutely_price_hour()

    def plot_prices(self, frequency: str):
        prices = self._choose_prices_frequency(frequency)
        prices[self._price_col].plot()
        plt.title(
            f"{frequency.capitalize()} {self._price_col} "
            f"for the {self._symbol} symbol"
        )
        plt.xticks(
            prices.resample("YS")[self._price_col].sum().index,
            ha="right",
            rotation=30,
            rotation_mode="anchor",
        )
        plt.show()

    def plot_price_changes_by_year(self, frequency, sharey=False):
        prices = self._choose_prices_frequency(frequency)
        yearly_resample = prices.resample("y")
        fig, axis = plt.subplots(
            len(yearly_resample),
            figsize=(20, 5 * len(yearly_resample)),
            sharey=sharey,
        )
        for i, year_prices in enumerate(yearly_resample[self._price_col]):
            year_prices[1].plot(ax=axis[i], title=year_prices[0].year)
        plt.suptitle(
            f"{frequency.capitalize()} {self._price_col} changes by year"
            f" for the {self._symbol} symbol",
            y=1.005,
        )
        plt.tight_layout()

    def plot_mean_price_day_of_month(self, frequency):
        prices = self._choose_prices_frequency(frequency)
        prices.groupby(prices.index.day)[self._price_col].mean().plot(
            kind="bar", rot=0
        )
        plt.xlabel("day of month")
        plt.title(
            f"Mean {frequency} {self._price_col} on different days of month"
        )
        plt.show()

    def plot_mean_price_day_of_week(self, frequency):
        prices = self._choose_prices_frequency(frequency)
        prices.groupby(prices.index.dayofweek)[self._price_col].mean().plot(
            kind="bar", rot=0
        )
        plt.xlabel("day of week")
        plt.title(
            f"Mean {frequency} {self._price_col} on different days of "
            f"week for the {self._symbol} symbol"
        )
        plt.show()

    def plot_minutely_price_hour(self):
        # TODO (Julia): maybe check this year by year in case there was
        # a change in the later years? E.g., trading pits closed.
        self.minutely_prices.groupby(self.minutely_prices.index.hour)[
            self._price_col
        ].mean().plot(kind="bar", rot=0)
        plt.title(
            f"Mean {self._price_col} during different hours "
            f"for the {self._symbol} symbol"
        )
        plt.xlabel("hour")
        plt.show()

    def _choose_prices_frequency(self, frequency):
        dbg.dassert_in(
            frequency,
            ["daily", "minutely"],
            "Only daily and minutely frequencies are supported.",
        )
        if frequency == "minutely":
            prices = self.minutely_prices
        else:
            prices = self.daily_prices
        return prices


class ProductSpecs:
    """
    Read product specs, get data by symbol or product group.
    """

    def __init__(self):
        self.product_specs = cmer.read_product_specs()

    def get_metadata_symbol(self, symbol):
        return self.product_specs.loc[self.product_specs["Globex"] == symbol]

    def get_trading_hours(self, symbol):
        # Only nans are repeated, so we can return the first element.
        return self.get_metadata_symbol(symbol)["Trading Hours"].iloc[0]

    def get_product_group(self, symbol):
        return self.get_metadata_symbol(symbol)["Product Group"].iloc[0]

    def get_specs_product_group(self, product_group):
        return self.product_specs.set_index("Product Group").loc[product_group]

    def get_symbols_product_group(self, product_group):
        return self.get_specs_product_group(product_group)["Globex"].values
