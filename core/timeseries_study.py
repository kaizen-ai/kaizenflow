"""
Import as:

import core.timeseries_study as tss
"""

import logging
from typing import Any, Callable, Dict, Iterable, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.statistics as stats
import helpers.dbg as dbg
import helpers.introspection as intr

_LOG = logging.getLogger(__name__)


class _TimeSeriesAnalyzer:
    """
    Perform basic study of time series, such as:
        - analysis at different time frequencies by resampling
        - plot time series for column
            - by year
            - by month
            - by day of week
            - by hour
    """

    def __init__(
        self,
        time_series: pd.Series,
        freq_name: str,
        data_name: Optional[str] = None,
        sharey: Optional[bool] = False,
        disabled_methods: Optional[Iterable[str]] = None,
    ):
        """
        :param time_series: pd.Series for which the study needs to be
            conducted
        :param freq_name: the name of the data frequency to add to plot titles
            e.g., 'daily'
        :param data_name: the name of the data to add to plot titles, e.g.,
            `symbol`
        :param sharey: a parameter passed into plt.subplots in `plot_by_year`
            method
        :param disabled_methods: methods that need to be skipped
        """
        dbg.dassert_isinstance(time_series, pd.Series)
        dbg.dassert_isinstance(time_series.index, pd.DatetimeIndex)
        dbg.dassert_monotonic_index(time_series.index)
        self._time_series = time_series
        self._ts_name = time_series.name
        self._data_name = data_name
        self._freq_name = freq_name
        self._sharey = sharey
        self._disabled_methods = disabled_methods or []

    def plot_time_series(self):
        """
        Plot timeseries on its original time scale.
        """
        func_name = intr.get_function_name()
        _LOG.debug(func_name)
        if func_name in self._disabled_methods:
            _LOG.debug("Skipping '%s' as per user request", func_name)
            return
        #
        self._time_series.plot()
        plt.title(
            f"{self._freq_name.capitalize()} {self._ts_name}"
            f"{self._title_suffix}"
        )
        plt.xticks(
            self._time_series.resample("YS").sum().index,
            ha="right",
            rotation=30,
            rotation_mode="anchor",
        )
        plt.show()

    def plot_by_year(self):
        """
        Resample yearly and then plot each year on a different plot.
        """
        func_name = intr.get_function_name()
        if self._need_to_skip(func_name):
            return
        #
        # Split by year.
        yearly_resample = self._time_series.resample("y")
        # Create as many subplots as years.
        _, axis = plt.subplots(
            len(yearly_resample),
            figsize=(20, 5 * len(yearly_resample)),
            sharey=self._sharey,
        )
        # Plot each year in a subplot.
        for i, year_ts in enumerate(yearly_resample):
            # resample() is a groupby() returning the value on which we
            # perform the split and the extracted time series.
            group_by, ts = year_ts
            ts.plot(ax=axis[i], title=group_by.year)
        plt.suptitle(
            f"{self._freq_name.capitalize()} {self._ts_name} by year"
            f"{self._title_suffix}",
            # Set suptitle at the top of the plots.
            y=1.005,
        )
        plt.tight_layout()
        plt.show()

    # TODO(gp): Think if it makes sense to generalize this by passing a lambda
    #  that define the timescale, e.g.,
    #   groupby_func = lambda ts: ts.index.day
    #  to sample on many frequencies (e.g., monthly, hourly, minutely, ...)
    #  The goal is to look for seasonality at many frequencies.
    # TODO(gp): It would be nice to plot the timeseries broken down by
    #  different timescales instead of doing the mean in each step
    #  (see https://en.wikipedia.org/wiki/Seasonality#Detecting_seasonality)
    def boxplot_day_of_month(self):
        """
        Plot the mean value of the timeseries for each day.
        """
        func_name = intr.get_function_name()
        if self._need_to_skip(func_name):
            return
        #
        self._boxplot(self._time_series, self._time_series.index.day)
        plt.xlabel("day of month")
        plt.title(
            f"{self._freq_name} {self._ts_name} on different days of "
            f"month{self._title_suffix}"
        )
        plt.show()
        # We need to close the plot otherwise there is a coupling between
        # plots that makes matplotlib assert.
        plt.close()

    def boxplot_day_of_week(self):
        """
        Plot the mean value of the timeseries for year.
        """
        func_name = intr.get_function_name()
        if self._need_to_skip(func_name):
            return
        #
        self._boxplot(self._time_series, self._time_series.index.dayofweek)
        plt.xlabel("day of week")
        plt.title(
            f"{self._freq_name} {self._ts_name} on different days of "
            f"week{self._title_suffix}"
        )
        plt.show()
        # We need to close the plot otherwise there is a coupling between
        # plots that makes matplotlib assert.
        plt.close()

    def execute(self):
        self.plot_time_series()
        self.plot_by_year()
        self.boxplot_day_of_month()
        self.boxplot_day_of_week()

    @staticmethod
    def _boxplot(ts, groupby):
        ts_df = pd.DataFrame(ts)
        ts_df["groupby"] = groupby
        ts_df.boxplot(by="groupby", column=ts.name)
        plt.suptitle("")

    def _need_to_skip(self, func_name):
        _LOG.debug(func_name)
        if func_name in self._disabled_methods:
            _LOG.debug("Skipping '%s' as per user request", func_name)
            return True
        return False

    @property
    def _title_suffix(self):
        if self._data_name is not None:
            ret = f" for {self._data_name}"
        else:
            ret = ""
        return ret


class TimeSeriesDailyStudy(_TimeSeriesAnalyzer):
    def __init__(
        self,
        time_series: pd.Series,
        data_name: Optional[str] = None,
        sharey: Optional[bool] = False,
        disabled_methods: Optional[Iterable[str]] = None,
    ):
        freq_name = "daily"
        super().__init__(
            time_series, freq_name, data_name, sharey, disabled_methods
        )


class TimeSeriesMinuteStudy(_TimeSeriesAnalyzer):
    def boxplot_minutely_hour(self):
        func_name = intr.get_function_name()
        if self._need_to_skip(func_name):
            return
        #
        self._boxplot(self._time_series, self._time_series.index.hour)
        plt.title(f"{self._ts_name} during different hours {self._title_suffix}")
        plt.xlabel("hour")
        plt.show()

    def execute(self):
        super().execute()
        self.boxplot_minutely_hour()


# Stats for time series.


# TODO(*): doubts - name is misleading and makes us believe that the function is
#  1) infering "the true period"
#  2) returns pd.timedelta and not the number of days
#  also the function migh be not general enough to be here
def infer_timedelta(series: pd.Series) -> int:
    """
    Compute timedelta for first two points of the time series.

    :return: timedelta as number of days between first 2 data points
    """
    if series.shape[0] > 1:
        timedelta = series.index[0] - series.index[1]
        timedelta = abs(timedelta.days)
    else:
        timedelta = np.nan
    return timedelta


def infer_average_timedelta(series: pd.Series) -> float:
    """
    Compute average timedelta based on beginning/end timestamps and sample counts.

    :return: timedelta as average number of days between all data points
    """
    timedelta = series.index.max() - series.index.min()
    return stats.safe_div(timedelta.days, series.shape[0])


def compute_moments(series: pd.Series) -> dict:
    """
    Wrap stats.moments function for returning a dict.

    :return: dict of moments
    """
    moments = stats.moments(series.dropna().to_frame()).to_dict(orient="records")[
        0
    ]
    return moments


def compute_coefficient_of_variation(series):
    """
    Compute the coefficient of variation for a given series.

    https://stats.stackexchange.com/questions/158729/normalizing-std-dev
    """
    return stats.safe_div(series.std(), series.mean())


# Functions for processing dict of time series to generate a df with statistics
# of these series.


def map_dict_to_dataframe(
    dict_: Dict[Any, pd.Series],
    functions: Dict[str, Callable],
    add_prefix: bool = True,
    progress_bar: bool = True,
) -> pd.DataFrame:
    """
    Apply and combine results of specified functions on a dict of series.

    :param dict_: dict of series to apply functions to.
    :param functions: dict with functions prefixes in keys and functions
        returns in values. Each function should receive a series as input
        and return a series or 1-column dataframe.
    :param add_prefix: bool value. If True, add specified prefixes to
        the functions outcomes and do not add if False.
    :param progress_bar: bool value. If True, show progress bar of applying
        the function to the series in the input and do not show if False.
    :return: dataframe with dict of series keys as column names and
         prefix + functions metrics' names as index.
    """
    all_func_outs = []
    dict_items = dict_.items()
    if progress_bar:
        dict_items = tqdm(dict_.items())
    for key, series in dict_items:
        # Apply all functions in `functions` to `series`.
        key_func_outs = []
        for prefix, func in functions.items():
            if not add_prefix:
                prefix = ""
            func_out = func(series, prefix=prefix)
            if isinstance(func_out, pd.DataFrame):
                func_out = func_out.squeeze("columns")
            dbg.dassert_isinstance(func_out, pd.Series)
            key_func_outs.append(func_out)
        # Create a single series from individual function series.
        key_func_out_srs = pd.concat(key_func_outs)
        key_func_out_srs.name = key
        all_func_outs.append(key_func_out_srs)
    # Concatenate each output series.
    df = pd.concat(all_func_outs, axis=1)
    return df
