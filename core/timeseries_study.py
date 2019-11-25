"""
Import as:

import core.timeseries_study as tss
"""

import logging
from typing import Iterable, List, Optional

import matplotlib.pyplot as plt
import pandas as pd

import helpers.dbg as dbg
import helpers.introspection as intr

_LOG = logging.getLogger(__name__)


class _Pipeline:
    """
    A class representing a sequence of functions that can be optionally
    disabled by the caller of `execute`.
    """

    def __init__(self, disable_methods: Optional[Iterable[str]] = None):
        # Extract the public methods of this class.
        self._methods = intr.get_methods(self, access="public")
        # Store the methods to disable.
        self._disable_methods = self._disable_methods_tolist(disable_methods)

    def execute(self):
        for method in self._methods:
            if method not in self._disable_methods:
                getattr(self, method)()
            else:
                _LOG.debug("Skipping %s as per user request", method)

    @staticmethod
    def _disable_methods_tolist(
        disable_methods: Optional[Iterable[str]],
    ) -> List[str]:
        disable_methods = disable_methods or []
        disable_methods = list(disable_methods)
        disable_methods.append("execute")
        return disable_methods


# TODO(gp): -> TimeSeriesAnalyzer?
class _TimeSeriesStudy(_Pipeline):
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
        disable_methods: Optional[Iterable[str]] = None,
    ):
        """
        :param time_series: pd.Series for which the study needs to be
            conducted
        :param freq_name: the name of the data frequency to add to plot
            titles (for example, 'daily')
        :param data_name: the name of the data to add to plot titles
            (for example, symbol)
        :param sharey: a parameter passed into plt.subplots in
            `plot_by_year` method
        :param disable_methods: methods that need to be skipped
        """
        dbg.dassert_isinstance(time_series, pd.Series)
        self._time_series = time_series
        self._ts_name = time_series.name
        self._data_name = data_name
        self._freq_name = freq_name
        self._sharey = sharey
        super().__init__(disable_methods)

    def plot_time_series(self):
        """
        Plot timeseries on its original time scale.
        """
        _LOG.debug(intr.get_function_name())
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
        _LOG.debug(intr.get_function_name())
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
        _LOG.debug(intr.get_function_name())
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
        _LOG.debug(intr.get_function_name())
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

    @staticmethod
    def _boxplot(ts, groupby):
        ts_df = pd.DataFrame(ts)
        ts_df["groupby"] = groupby
        ts_df.boxplot(by="groupby", column=ts.name)
        plt.suptitle("")

    def _check_data_index(self):
        dbg.dassert_isinstance(self._time_series.index, pd.DatetimeIndex)
        dbg.dassert_monotonic_index(self._time_series.index)

    @property
    def _title_suffix(self):
        if self._data_name is not None:
            ret = f" for {self._data_name}"
        else:
            ret = ""
        return ret


# TODO(gp): Not sure this is needed if we generalize the super class to work
#  at different frequency. We can have a check that makes sure we always
#  downsample (e.g., we don't plot at minutely timescale if the frequency of the
#  timeseries is hourly).
class TimeSeriesDailyStudy(_TimeSeriesStudy):
    def __init__(
        self,
        time_series: pd.Series,
        freq_name: Optional[str] = None,
        data_name: Optional[str] = None,
        disable_methods: Optional[Iterable[str]] = None,
    ):
        if not freq_name:
            freq_name = "daily"
        super().__init__(time_series, freq_name, data_name, disable_methods)


class TimeSeriesMinuteStudy(_TimeSeriesStudy):
    def __init__(
        self,
        time_series: pd.Series,
        freq_name: Optional[str] = None,
        data_name: Optional[str] = None,
        disable_methods: Optional[Iterable[str]] = None,
    ):
        if not freq_name:
            freq_name = "minutely"
        super().__init__(time_series, freq_name, data_name, disable_methods)

    def boxplot_minutely_hour(self):
        _LOG.debug(intr.get_function_name())
        self._boxplot(self._time_series, self._time_series.index.hour)
        plt.title(f"{self._ts_name} during different hours {self._title_suffix}")
        plt.xlabel("hour")
        plt.show()
