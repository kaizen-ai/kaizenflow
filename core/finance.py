import datetime
import logging
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
import statsmodels.api as sm

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


SR_COL = "sharpe"
RET_0_COL = "ret_0"


def resample_1min(df, skip_weekends):
    """
    Resample a df to one minute resolution leaving np.nan for the empty minutes.
    Note that this is done on a 24h / calendar basis, without accounting for
    any trading times.
    :param skip_weekends: remove Sat and Sun
    :return: resampled df
    """
    dbg.dassert_monotonic_index(df)
    date_range = pd.date_range(
        start=df.index.min(), end=df.index.max(), freq="1T"
    )
    # Remove weekends.
    if skip_weekends:
        # TODO(gp): Use thej proper calendar.
        # date_range = [d for d in df.index if d.date().weekday() < 5]
        mask = [d.weekday() < 5 for d in date_range]
        date_range = date_range[mask]
    df = df.reindex(date_range)
    return df


def remove_dates_with_no_data(df, report_stats):
    """
    Given a df indexed with timestamps, scan the data by date and filter out
    all the data when it's all nans.
    :return: filtered df
    """
    # This is not strictly necessary.
    dbg.dassert_monotonic_index(df)
    #
    removed_days = []
    df_out = []
    num_days = 0
    for date, df_tmp in df.groupby(df.index.date):
        if np.isnan(df_tmp).all(axis=1).all():
            _LOG.debug("No data on %s", date)
            removed_days.append(date)
        else:
            df_out.append(df_tmp)
        num_days += 1
    df_out = pd.concat(df_out)
    dbg.dassert_monotonic_index(df_out)
    #
    if report_stats:
        _LOG.info("df.index in [%s, %s]", df.index.min(), df.index.max())
        removed_perc = pri.perc(df.shape[0] - df_out.shape[0], df.shape[0])
        _LOG.info("Rows removed: %s", removed_perc)
        #
        removed_perc = pri.perc(len(removed_days), num_days)
        _LOG.info("Number of removed days: %s", removed_perc)
        # Find week days.
        removed_weekdays = [d for d in removed_days if d.weekday() < 5]
        removed_perc = pri.perc(len(removed_weekdays), len(removed_days))
        _LOG.info("Number of removed weekdays: %s", removed_perc)
        _LOG.info("Weekdays removed: %s", ", ".join(map(str, removed_weekdays)))
        #
        removed_perc = pri.perc(
            len(removed_days) - len(removed_weekdays), len(removed_days)
        )
        _LOG.info("Number of removed weekend days: %s", removed_perc)
        #

    return df_out


def resample(df, agg_interval):
    """
    Resample returns (using sum) using our timing convention.
    """
    dbg.dassert_monotonic_index(df)
    resampler = df.resample(agg_interval, closed="left", label="right")
    rets = resampler.sum()
    return rets


# TODO(GPP): DEPRECATE. PyCharm doesn't find any callers, and we are
# using "result bundles" differently now.
def filter_by_time(
    df,
    start_dt,
    end_dt,
    result_bundle=None,
    dt_col_name=None,
    log_level=logging.INFO,
):
    dbg.dassert_lte(1, df.shape[0])
    if start_dt is not None and end_dt is not None:
        dbg.dassert_lte(start_dt, end_dt)
    #
    if start_dt is not None:
        _LOG.log(log_level, "Filtering df with start_dt=%s", start_dt)
        if dt_col_name:
            mask = df[dt_col_name] >= start_dt
        else:
            mask = df.index >= start_dt
        kept_perc = pri.perc(mask.sum(), df.shape[0])
        _LOG.info(">= start_dt=%s: kept %s rows", start_dt, kept_perc)
        if result_bundle:
            result_bundle["filter_ge_start_dt"] = kept_perc
        df = df[mask]
    #
    if end_dt is not None:
        _LOG.info("Filtering df with end_dt=%s", end_dt)
        if dt_col_name:
            mask = df[dt_col_name] < end_dt
        else:
            mask = df.index < end_dt
        kept_perc = pri.perc(mask.sum(), df.shape[0])
        _LOG.info("< end_dt=%s: kept %s rows", end_dt, kept_perc)
        if result_bundle:
            result_bundle["filter_lt_end_dt"] = kept_perc
        df = df[mask]
    dbg.dassert_lte(1, df.shape[0])
    return df


def set_non_ath_to_nan(
    df: pd.DataFrame,
    start_time: Optional[datetime.time] = None,
    end_time: Optional[datetime.time] = None,
) -> pd.DataFrame:
    """
    Filter according to active trading hours.

    We assume time intervals are left closed, right open, labeled right.

    Row is not set to `np.nan` iff its `time` satisifies
      - `start_time < time`, and
      - `time <= end_time`
    """
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    if start_time is None:
        start_time = datetime.time(9, 30)
    if end_time is None:
        end_time = datetime.time(16, 0)
    dbg.dassert_lte(start_time, end_time)
    #
    times = df.index.time
    mask = (start_time < times) & (times <= end_time)
    #
    df = df.copy()
    df[~mask] = np.nan
    return df


# TODO(gp): ATHs vary over futures. Use volume to estimate them.
def filter_ath(
    df: pd.DataFrame, dt_col_name: Optional[Any] = None
) -> pd.DataFrame:
    """
    Filter according to active trading hours.
    """
    dbg.dassert_lte(1, df.shape[0])
    if dt_col_name:
        times = np.array([dt.time() for dt in df[dt_col_name]])
    else:
        # Use index.
        # times = np.array([dt.time() for dt in df.index])
        times = df.index.time
    # Note that we need to exclude time(16, 0) since the last bar is tagged
    # with time(15, 59).
    # TODO(gp): Pass this values since they depend on the interval conventions.
    start_time = datetime.time(9, 30)
    end_time = datetime.time(15, 59)
    mask = (start_time <= times) & (times <= end_time)
    #
    df = df.copy()
    df = df[mask]
    dbg.dassert_lte(1, df.shape[0])
    return df


def show_distribution_by(by, ascending=False):
    by = by.sort_values(ascending=ascending)
    by.plot(kind="bar")


# #############################################################################
# Pnl returns stats.
# #############################################################################


def compute_ret_0(
    prices: Union[pd.Series, pd.DataFrame], mode: str
) -> Union[pd.Series, pd.DataFrame]:
    if mode == "pct_change":
        ret_0 = prices.pct_change()
    elif mode == "log_rets":
        ret_0 = np.log(prices) - np.log(prices.shift(1))
    elif mode == "diff":
        # TODO(gp): Use shifts for clarity, e.g.,
        # ret_0 = prices - prices.shift(1)
        ret_0 = prices.diff()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    if isinstance(ret_0, pd.Series):
        ret_0.name = RET_0_COL
    return ret_0


def compute_ret_0_from_multiple_prices(
    prices: Dict[str, pd.DataFrame], col_name: str, mode: str
) -> pd.DataFrame:
    dbg.dassert_isinstance(prices, dict)
    rets = []
    for s, price_df in prices.items():
        _LOG.debug("Processing s=%s", s)
        rets_tmp = compute_ret_0(price_df[col_name], mode)
        rets_tmp = pd.DataFrame(rets_tmp)
        rets_tmp.columns = ["%s_ret_0" % s]
        rets.append(rets_tmp)
    rets = pd.concat(rets, sort=True, axis=1)
    return rets


# TODO(*): Refactor this so that we
# - have a core function that calculates returns by subtracting one
#   series from the other, with all of the `mode` options
# - have `compute_ret_0` wrap this core function, calling it with the
#   original series and a shifted series
# - have compute_first_causal_lag wrap the core function, calling it
#   after shifting each series
def compute_first_causal_lag(
    lhs: pd.Series, rhs: pd.Series, mode: str
) -> pd.Series:
    """
    Given the semantic of some price data sets (e.g., Kibot) the first
    "causal" return we can use to trade ret_0 is the
    TODO(gp): finish this

    :param lhs: series from which the `rhs` is
        subtracted
    :param rhs: series is subtracted from the
        `minuend_series`
    :param mode: `pct_change`, `log_rets` or `diff`
    :return: returns series
    """
    # TODO(GPP): Consider using fin.compute_ret_0 and then shifting.
    if mode == "pct_change":
        rets = lhs.shift(1) - rhs.shift(1)
        rets /= rhs.shift(1)
    elif mode == "log_rets":
        rets = lhs.shift(1) - rhs.shift(1)
        rets = np.log(1 + rets)
    elif mode == "diff":
        rets = lhs.shift(1) - rhs.shift(1)
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    rets.name = "ret_1_star"
    return rets


# TODO(GPPJ): Add a decorator for handling multi-variate prices as in
#  https://github.com/ParticleDev/commodity_research/issues/568


def convert_log_rets_to_pct_rets(
    log_rets: Union[pd.Series, pd.DataFrame]
) -> Union[pd.Series, pd.DataFrame]:
    """
    Convert log returns to percentage returns.

    :param log_rets: time series of log returns
    :return: time series of percentage returns
    """
    return np.exp(log_rets) - 1


def convert_pct_rets_to_log_rets(
    pct_rets: Union[pd.Series, pd.DataFrame]
) -> Union[pd.Series, pd.DataFrame]:
    """
    Convert percentage returns to log returns.

    :param pct_rets: time series of percentage returns
    :return: time series of log returns
    """
    return np.log(pct_rets + 1)


def compute_sharpe_ratio(
    log_rets: Union[pd.Series, pd.DataFrame], time_scaling: Union[int, float] = 1
) -> Union[float, pd.Series]:
    r"""
    Calculate Sharpe Ratio (SR) from log returns and rescale.

    For a detailed exploration of SR, see
    http://www.gilgamath.com/pages/ssc.html.

    :param log_rets: time series of log returns
    :param time_scaling: rescales SR by a factor of \sqrt(time_scaling).
        - For SR with respect to the sampling frequency, set equal to 1
        - For annualization, set equal to the number of sampling frequency
          ticks per year (e.g., =252 if daily returns are provided)
    :return: Sharpe Ratio
    """
    dbg.dassert_lte(1, time_scaling, "Time scaling factor must be positive!")
    sr = log_rets.mean() / log_rets.std()
    sr *= np.sqrt(time_scaling)
    if isinstance(sr, pd.Series):
        sr.name = SR_COL
    return sr


# TODO(Paul): Consider renaming.
def compute_sr(rets):
    """
    NOTE: The current implementation of this resamples to daily but does not
    filter out non-trading days. This will tend to deflate the SR.

    See also compute_rolling_sharpe_ratio in signal_processing.py

    We can also use tools in bayesian.py for a more comprehensive assessment.
    """
    # Annualize (brutally).
    # sr = rets.mean() / rets.std()
    # sr *= np.sqrt(252 * ((16 - 9.5) * 60))
    daily_rets = rets.resample("1D").sum()
    sr = compute_sharpe_ratio(daily_rets, 252)
    return sr


def compute_kratio(rets, y_var):
    # From http://s3.amazonaws.com/zanran_storage/www.styleadvisor.com/
    #   ContentPages/2449998087.pdf
    daily_rets = rets.resample("1B").sum().cumsum()
    # Fit the best line to the daily rets.
    x = range(daily_rets.shape[0])
    x = sm.add_constant(x)
    y = daily_rets[y_var]
    reg = sm.OLS(y, x)
    model = reg.fit()
    # Compute k-ratio as slope / std err of slope.
    kratio = model.params[1] / model.bse[1]
    if False:
        # Debug the function.
        print(model.summary())
        daily_rets.index = range(daily_rets.shape[0])
        daily_rets["kratio"] = model.predict(x)
        daily_rets.plot()
    return kratio


def _compute_drawdown(log_rets: pd.Series) -> pd.Series:
    r"""
    Calculate drawdown of a time series of log returns.

    Define the drawdown at index location j to be
        d_j := max_{0 \leq i \leq j} \log(p_i / p_j)
    where p_k is price. Though this definition is in terms of prices, we
    calculate the drawdown series using log returns.

    Using this definition, drawdown is always nonnegative.

    :param log_rets: time series of log returns
    :return: drawdown time series

    # TODO(Paul): Extend this to dataframes
    """
    dbg.dassert_isinstance(log_rets, pd.Series)
    # Keep track of maximum drawdown ending at index location i + 1.
    sums = log_rets.dropna()
    for i in range(sums.shape[0] - 1):
        if sums.iloc[i] <= 0:
            sums.iloc[i + 1] += sums.iloc[i]
    # Correction for case where max occurs at i == j.
    sums[sums > 0] = 0
    # Sign normalization.
    return -sums


# TODO(Paul): Extend to DataFrames.
def compute_perc_loss_from_high_water_mark(log_rets: pd.Series) -> pd.Series:
    """
    Calculate drawdown in terms of percentage loss.

    :param log_rets: time series of log returns
    :return: drawdown time series as percentage loss
    """
    dd = _compute_drawdown(log_rets)
    return 1 - np.exp(-dd)
