"""
Import as:

import core.timeseries_study as tss
"""

import logging
from typing import Iterable, Optional

import matplotlib.pyplot as plt
import pandas as pd

import helpers.dbg as dbg
import helpers.introspection as intr
from typing import Dict, Callable

_LOG = logging.getLogger(__name__)


# TODO(gp): -> TimeSeriesAnalyzer?
class _TimeSeriesStudy:
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


class TimeSeriesDailyStudy(_TimeSeriesStudy):
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


class TimeSeriesMinuteStudy(_TimeSeriesStudy):

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


# Functions for processing dict of time series to generate a df with statistics
# of these series.


def compute_single_metadata(metadata_functions: Dict[str, Callable],
                            series: pd.Series) -> Dict:
    """
    Iterates over the dict of metadata functions and applies them to the series.

    If some function's returns a dict, iterate on it and add an element to
    the computed_metadata dict
    :return: dict of metrics
    """
    computed_metadata = {}
    for metric, function in metadata_functions.items():
        result = function(series)
        if type(result) == dict:
            for k, v in result.items():
                computed_metadata[k] = v
        else:
            computed_metadata[metric] = result
    return computed_metadata


def compute_metadata(metadata_functions: Dict[str, Callable],
                     timeseries_data: Dict[str, pd.Series]) -> Dict:
    """
    Apply compute_single_metadata to multple time series from timeseries_data.

    :return: metadata df indexed by series_id with computed metrics as columns
    """
    generated_metadata = {
        series_id: compute_single_metadata(metadata_functions, series) for
        series_id, series in timeseries_data.items()}
    return pd.DataFrame.from_dict(generated_metadata, orient='index')