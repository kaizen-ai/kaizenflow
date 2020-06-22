import datetime
import logging
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import statsmodels.api as sm

import helpers.dataframe as hdf
import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


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
    dbg.dassert_monotonic_index(df)
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


def set_weekends_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out weekends.
    """
    dbg.dassert_isinstance(df.index, pd.DatetimeIndex)
    # 5 = Saturday, 6 = Sunday.
    mask = df.index.day.isin([5, 6])
    df = df.copy()
    df[mask] = np.nan
    return df


# #############################################################################
# Returns calculation and helpers.
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
        ret_0.name = "ret_0"
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


def rescale_to_target_annual_volatility(
    srs: pd.Series, volatility: float
) -> pd.Series:
    """
    Rescale srs to achieve target annual volatility.

    NOTE: This is not a causal rescaling, but SR is an invariant.

    :param srs: returns series. Index must have `freq`.
    :param volatility: annualized volatility as a proportion (e.g., `0.1`
        corresponds to 10% annual volatility)
    :return: rescaled returns series
    """
    dbg.dassert_isinstance(srs, pd.Series)
    ppy = hdf.infer_sampling_points_per_year(srs)
    srs = hdf.apply_nan_mode(srs, mode="fill_with_zero")
    scale_factor = volatility / (np.sqrt(ppy) * srs.std())
    _LOG.debug("`scale_factor`=%f", scale_factor)
    return scale_factor * srs


def aggregate_log_rets(df: pd.DataFrame, target_volatility: float) -> pd.Series:
    """
    Perform inverse variance weighting and normalize volatility.

    :param df: cols contain log returns
    :param target_volatility: annualize target volatility
    :return: srs of log returns
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    df = df.apply(
        lambda x: rescale_to_target_annual_volatility(x, target_volatility)
    )
    df = df.apply(convert_log_rets_to_pct_rets)
    df = df.mean(axis=1)
    srs = df.squeeze()
    srs = convert_pct_rets_to_log_rets(srs)
    return srs


# TODO(*): Consider moving to `statistics.py`.
# #############################################################################
# Returns stats.
# #############################################################################


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


def compute_drawdown(log_rets: pd.Series) -> pd.Series:
    r"""
    Calculate drawdown of a time series of log returns.

    Define the drawdown at index location j to be
        d_j := max_{0 \leq i \leq j} \log(p_i / p_j)
    where p_k is price. Though this definition is in terms of prices, we
    calculate the drawdown series using log returns.

    Using this definition, drawdown is always nonnegative.

    :param log_rets: time series of log returns
    :return: drawdown time series
    """
    dbg.dassert_isinstance(log_rets, pd.Series)
    log_rets = hdf.apply_nan_mode(log_rets, mode="fill_with_zero")
    cum_rets = log_rets.cumsum()
    running_max = np.maximum.accumulate(cum_rets)
    drawdown = running_max - cum_rets
    return drawdown


def compute_perc_loss_from_high_water_mark(log_rets: pd.Series) -> pd.Series:
    """
    Calculate drawdown in terms of percentage loss.

    :param log_rets: time series of log returns
    :return: drawdown time series as percentage loss
    """
    dd = compute_drawdown(log_rets)
    return 1 - np.exp(-dd)


def compute_turnover(pos: pd.Series, nan_mode: Optional[str] = None) -> pd.Series:
    """
    Compute turnover for a sequence of positions.

    :param pos: sequence of positions
    :param nan_mode: argument for hdf.apply_nan_mode()
    :return: turnover
    """
    dbg.dassert_isinstance(pos, pd.Series)
    nan_mode = nan_mode or "ffill"
    pos = hdf.apply_nan_mode(pos, mode=nan_mode)
    numerator = pos.diff().abs()
    denominator = (pos.abs() + pos.shift().abs()) / 2
    turnover = numerator / denominator
    return turnover


def compute_average_holding_period(
    pos: pd.Series, unit: Optional[str] = None, nan_mode: Optional[str] = None
) -> pd.Series:
    """
    Compute average holding period for a sequence of positions.

    :param pos: sequence of positions
    :param unit: desired output unit (e.g. 'D', 'W', 'M', etc.)
    :param nan_mode: argument for hdf.apply_nan_mode()
    :return: average holding period in specified units
    """
    unit = unit or "D"
    dbg.dassert_isinstance(pos, pd.Series)
    dbg.dassert(pos.index.freq)
    # TODO (Dan): Find a better way to compare frequencies.
    # The way to move frequencies to a computable form.
    #    https://stackoverflow.com/questions/24635721/how-to-compare-frequencies-sampling-rates-in-pandas
    dummy_date = pd.to_datetime("2000-01-01")
    pos_freq = (
        dummy_date + pd.tseries.frequencies.to_offset(pos.index.freq) - dummy_date
    )
    unit_freq = dummy_date + pd.tseries.frequencies.to_offset(unit) - dummy_date
    #
    dbg.dassert_lte(pos_freq, unit_freq)
    nan_mode = nan_mode or "ffill"
    pos = hdf.apply_nan_mode(pos, mode=nan_mode)
    unit_coef = pos_freq / unit_freq
    average_holding_period = (
        pos.abs().mean() / pos.diff().abs().mean()
    ) * unit_coef
    return average_holding_period