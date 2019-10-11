import logging

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from pylab import rcParams

from vendors.kibot import utils as kut

sns.set()

rcParams["figure.figsize"] = (20, 5)

_LOG = logging.getLogger(__name__)

_PRODUCT_SPECS_PATH = (
    "/data/prices/product_slate_export_with_contract_specs_20190905.csv"
)


def get_sum_daily_volume(daily_price_dict_df):
    """
    Get sum of the daily volume for each symbol

    :param daily_price_dict_df: {symbol: prices_for_symbol_df}
    :return: pd.DataFrame indexed by symbol
    """
    daily_volume_sum_dict = {
        symbol: daily_prices_symbol["vol"].sum()
        for symbol, daily_prices_symbol in daily_price_dict_df.items()
    }
    daily_volume_sum_df = pd.DataFrame.from_dict(
        daily_volume_sum_dict, orient="index", columns=["sum_vol"]
    )
    daily_volume_sum_df.index.name = "symbol"
    return daily_volume_sum_df


def get_mean_daily_volume(daily_price_dict_df):
    """
    Get mean of the daily volume for each symbol

    :param daily_price_dict_df: {symbol: prices_for_symbol_df}
    :return: pd.DataFrame indexed by symbol
    """
    daily_volume_mean_dict = {
        symbol: daily_prices_symbol["vol"].mean()
        for symbol, daily_prices_symbol in daily_price_dict_df.items()
    }
    daily_volume_mean_df = pd.DataFrame.from_dict(
        daily_volume_mean_dict, orient="index", columns=["mean_vol"]
    )
    daily_volume_mean_df.index.name = "symbol"
    return daily_volume_mean_df


class VolumeStudy:
    """
    Perform a basic volume study of daily and minutely prices.

    - Read daily and minutely prices
    - Plot daily and minutely volume
        - by year
        - by month
        - by day of week
        - by hour
    """

    def __init__(self, symbol, n_rows):
        """
        :param symbol: the symbol for which the volume needs to be
            studied
        :param n_rows: the maximum numer of rows to load
        """
        self.symbol = symbol
        self.n_rows = n_rows
        self.daily_prices = self._read_daily_prices()
        self.minutely_prices = self._read_minutely_prices()

    def execute(self):
        self._plot_daily_volume()
        self._plot_daily_volume_changes_by_year()
        self._mean_daily_volume_day_of_month()
        self._mean_daily_volume_day_of_week()
        #
        self._plot_minutely_volume()
        self._plot_minutely_volume_changes_by_year()
        self._plot_minutely_volume_day_of_week()
        self._plot_minutely_volume_hour()

    def _read_daily_prices(self):
        file_name = "/data/kibot/All_Futures_Continuous_Contracts_daily/%s.csv.gz"
        prices_symbol = kut.read_data(file_name % self.symbol, nrows=self.n_rows)
        return prices_symbol

    def _read_minutely_prices(self):
        file_name = "/data/kibot/All_Futures_Continuous_Contracts_1min/%s.csv.gz"
        mins_prices_symbol = kut.read_data(
            file_name % self.symbol, nrows=self.n_rows
        )
        return mins_prices_symbol

    def _plot_daily_volume(self):
        self.daily_prices["vol"].plot()
        plt.title(f"Daily volume for the {self.symbol} symbol")
        plt.xticks(
            self.daily_prices.resample("YS")["vol"].sum().index,
            ha="right",
            rotation=30,
            rotation_mode="anchor",
        )
        plt.show()

    def _plot_daily_volume_changes_by_year(self, sharey=False):
        yearly_resample = self.daily_prices.resample("y")
        fig, axis = plt.subplots(
            len(yearly_resample),
            figsize=(20, 5 * len(yearly_resample)),
            sharey=sharey,
        )
        for i, year_vol in enumerate(yearly_resample["vol"]):
            year_vol[1].plot(ax=axis[i], title=year_vol[0].year)
        plt.suptitle(
            f"Daily volume changes by year for the {self.symbol} symbol", y=1.005
        )
        plt.tight_layout()

    def _mean_daily_volume_day_of_month(self):
        self.daily_prices.groupby(self.daily_prices.index.day)["vol"].mean().plot(
            kind="bar", rot=0
        )
        plt.xlabel("day of month")
        plt.title("Mean volume on different days of month")
        plt.show()

    def _mean_daily_volume_day_of_week(self):
        self.daily_prices.groupby(self.daily_prices.index.dayofweek)[
            "vol"
        ].mean().plot(kind="bar", rot=0)
        plt.xlabel("day of week")
        plt.title(
            f"Mean volume on different days of week for the "
            f"{self.symbol} symbol"
        )
        plt.show()

    def _plot_minutely_volume(self):
        self.minutely_prices["vol"].plot()
        plt.xticks(
            self.minutely_prices.resample("YS")["vol"].sum().index,
            ha="right",
            rotation=30,
            rotation_mode="anchor",
        )
        plt.title(f"Minutely volume for the {self.symbol} symbol")
        plt.show()

    def _plot_minutely_volume_changes_by_year(self):
        yearly_resample = self.minutely_prices.resample("y")
        fig, axis = plt.subplots(
            len(yearly_resample), figsize=(20, 5 * len(yearly_resample))
        )
        for i, year_vol in enumerate(yearly_resample["vol"]):
            year_vol[1].plot(ax=axis[i], title=year_vol[0].year)
        plt.suptitle(
            f"Minutely mean volume changes by year for the "
            f"{self.symbol} symbol",
            y=1.005,
        )
        plt.tight_layout()

    def _plot_minutely_volume_day_of_week(self):
        self.minutely_prices.groupby(self.minutely_prices.index.dayofweek)[
            "vol"
        ].mean().plot(kind="bar", rot=0)
        plt.xlabel("day of week")
        plt.title(
            f"Mean volume on different days of week for the "
            f"{self.symbol} symbol"
        )
        plt.show()

    def _plot_minutely_volume_hour(self):
        # TODO (Julia): maybe check this year by year in case there was
        # a change in the later years? E.g., trading pits closed
        self.minutely_prices.groupby(self.minutely_prices.index.hour)[
            "vol"
        ].mean().plot(kind="bar", rot=0)
        plt.title(
            f"Mean volume during different hours for the {self.symbol} symbol"
        )
        plt.xlabel("hour")
        plt.show()


class ProductSpecs:
    """
    Read product specs, get data by symbol or product group.
    """

    def __init__(self):
        self.product_specs = pd.read_csv(_PRODUCT_SPECS_PATH)

    def get_metadata_symbol(self, symbol):
        return self.product_specs.loc[self.product_specs["Globex"] == symbol]

    def get_trading_hours(self, symbol):
        # Only nans are repeated, so we can return the first element
        return self.get_metadata_symbol(symbol)["Trading Hours"].iloc[0]

    def get_product_group(self, symbol):
        return self.get_metadata_symbol(symbol)["Product Group"].iloc[0]

    def get_specs_product_group(self, product_group):
        return self.product_specs.set_index("Product Group").loc[product_group]

    def get_symbols_product_group(self, product_group):
        return self.get_specs_product_group(product_group)["Globex"].values
