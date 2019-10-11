import datetime
import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


SR_COL = "sharpe"


def zscore(obj, com, demean, standardize, delay, min_periods=None):
    """
    DEPRECATE in favor of rolling_zscore in signal_processing.py.
    """
    dbg.dassert_type_in(obj, (pd.Series, pd.DataFrame))
    # z-scoring might not be causal with delay=0, especially for predicted
    # variables.
    dbg.dassert_lte(0, delay)
    # TODO(gp): Extend this to series.
    dbg.dassert_monotonic_index(obj)
    obj = obj.copy()
    if min_periods is None:
        min_periods = 3 * com
    if demean:
        mean = obj.ewm(com=com, min_periods=min_periods).mean()
        if delay != 0:
            mean = mean.shift(delay)
        # TODO: Check the logic here (if demean=True, standardize=True, and
        # delay > 0, then we end up shifting an already-shifted series...
        obj = obj - mean
    if standardize:
        # TODO(gp): Remove nans, if needed.
        std = obj.ewm(com=com, min_periods=min_periods).std()
        if delay != 0:
            std = std.shift(delay)
        obj = obj / std
    return obj


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


# TODO(gp): ATHs vary over futures. Use volume to estimate them.
def filter_ath(df, dt_col_name=None, log_level=logging.INFO):
    """
    Filter according to active trading hours.
    """
    dbg.dassert_lte(1, df.shape[0])
    _LOG.log(log_level, "Filtering by ATH")
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
    df = df[mask]
    # Need to make a copy to avoid warnings
    #   "A value is trying to be set on a copy of a slice from a DataFrame"
    # downstream
    df = df.copy()
    _LOG.log(log_level, "df.shape=%s", df.shape)
    dbg.dassert_lte(1, df.shape[0])
    return df


def show_distribution_by(by, ascending=False):
    by = by.sort_values(ascending=ascending)
    by.plot(kind="bar")


# #############################################################################
# Pnl returns stats.
# #############################################################################


def log_rets_to_rets(df):
    return np.exp(df) - 1


def compute_sharpe_ratio(df, time_scaling=1):
    sr = df.mean() / df.std()
    sr *= np.sqrt(time_scaling)
    if isinstance(sr, pd.Series):
        sr.name = SR_COL
    return sr


def annualize_sharpe_ratio(df):
    return compute_sr(df)


def compute_sr(rets):
    """
    NOTE: The current implementation of this resamples to daily but does not
    filter out non-trading days. This will tend to deflate the SR.

    See also rolling_sharpe_ratio in signal_processing.py

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


def _compute_drawdown(log_rets):
    r"""
    Define the drawdown at index location j to be
        d_j := max_{0 \leq i \leq j} \log(p_i / p_j)
    where p_k is price. Though this definition is in terms of prices, we
    calculate the drawdown series using log returns.

    Using this definition, drawdown is always nonnegative.

    :param log_rets: log returns
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


def compute_max_perc_loss_from_high_water_mark(log_rets):
    dd = _compute_drawdown(log_rets)
    return 1 - np.exp(-dd)
