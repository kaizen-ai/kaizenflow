"""Import as:

import core.timeseries_study as tss
"""

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import pandas as pd
from tqdm.auto import tqdm

import core.dataflow as dtf
import helpers.dbg as dbg
import helpers.introspection as intr

_LOG = logging.getLogger(__name__)


class DataFrameModeler:
    """
    Wraps common dataframe modeling and exploratory analysis functionality.

    TODO(*): Add
      - seasonal decomposition
      - stats (e.g., stationarity, autocorrelation)
      - correlation / clustering options
    """

    def __init__(
        self, df: pd.DataFrame, oos_start: Optional[float] = None,
    ) -> None:
        """
        Initialize by supplying a dataframe of time series.

        :param df: time series dataframe
        :param oos_start: Optional end of in-sample/start of out-of-sample.
            For methods supporting "fit"/"predict", "fit" applies to
            in-sample only, and "predict" requires `oos_start`.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert(pd.DataFrame)
        self.df = df
        self.oos_start = oos_start or None

    def apply_sklearn_model(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        y_vars: Union[List[str], Callable[[], List[str]]],
        steps_ahead: int,
        model_kwargs: Optional[Any] = None,
        mode: str = "fit",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Apply a supervised sklearn model.

        Both x and y vars should be indexed by knowledge time.
        """
        model = dtf.ContinuousSkLearnModel(
            nid="sklearn",
            model_func=model_func,
            x_vars=x_vars,
            y_vars=y_vars,
            steps_ahead=steps_ahead,
            model_kwargs=model_kwargs,
            col_mode="merge_all",
            nan_mode="drop",
        )
        return self._run_model(model, mode)

    def apply_unsupervised_sklearn_model(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        model_kwargs: Optional[Any] = None,
        mode: str = "fit",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Apply an unsupervised model, e.g., PCA.
        """
        model = dtf.UnsupervisedSkLearnModel(
            nid="unsupervised_sklearn",
            model_func=model_func,
            x_vars=x_vars,
            model_kwargs=model_kwargs,
            col_mode="merge_all",
            nan_mode="drop",
        )
        return self._run_model(model, mode)

    def apply_residualizer(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        model_kwargs: Optional[Any] = None,
        mode: str = "fit",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Apply an unsupervised model and residualize.
        """
        model = dtf.Residualizer(
            nid="sklearn_residualizer",
            model_func=model_func,
            x_vars=x_vars,
            model_kwargs=model_kwargs,
            nan_mode="drop",
        )
        return self._run_model(model, mode)

    def apply_sma_model(
        self,
        col: str,
        steps_ahead: int,
        tau: Optional[float] = None,
        mode: str = "fit",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Apply a smooth moving average model.
        """
        model = dtf.SmaModel(
            nid="sma_model",
            col=[col],
            steps_ahead=steps_ahead,
            tau=tau,
            nan_mode="drop",
        )
        return self._run_model(model, mode)

    def apply_volatility_model(
        self,
        col: str,
        steps_ahead: int,
        p_moment: float = 2,
        tau: Optional[float] = None,
        mode: str = "fit",
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Model volatility.
        """
        model = dtf.VolatilityModel(
            nid="volatility_model",
            col=[col],
            steps_ahead=steps_ahead,
            p_moment=p_moment,
            tau=tau,
            nan_mode="drop",
        )
        return self._run_model(model, mode)

    def _run_model(self, model, mode) -> Tuple[pd.DataFrame, dict]:
        if mode == "fit":
            df_out = model.fit(self.df[: self.oos_start])["df_out"]
            info = model.get_info("fit")
        elif mode == "predict":
            model.fit(self.df[self.oos_start :])
            df_out = model.predict(self.df)["df_out"]
            info = model.get_info("predict")
        else:
            raise ValueError(f"Unrecognized mode `{mode}`.")
        return df_out, info


class _TimeSeriesAnalyzer:
    """Perform basic study of time series, such as:

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
        dbg.dassert_strictly_increasing_index(time_series.index)
        self._time_series = time_series
        self._ts_name = time_series.name
        self._data_name = data_name
        self._freq_name = freq_name
        self._sharey = sharey
        self._disabled_methods = disabled_methods or []

    def plot_time_series(self):
        """Plot timeseries on its original time scale."""
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
        """Resample yearly and then plot each year on a different plot."""
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
        """Plot the mean value of the timeseries for each day."""
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
        """Plot the mean value of the timeseries for year."""
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


class TimeSeriesMinutelyStudy(_TimeSeriesAnalyzer):
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


def map_dict_to_dataframe(
    dict_: Dict[Any, pd.Series],
    functions: Dict[str, Callable],
    add_prefix: bool = True,
    progress_bar: bool = True,
) -> pd.DataFrame:
    """Apply and combine results of specified functions on a dict of series.

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
