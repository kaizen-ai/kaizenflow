"""Import as:

import core.statistics as stats
"""

import collections
import datetime
import functools
import logging
import math
import numbers
from typing import Any, Iterable, List, Optional, Tuple, Union

import core.finance as fin
import helpers.dataframe as hdf
import helpers.dbg as dbg
import numpy as np
import pandas as pd
import scipy as sp
import sklearn.model_selection
import statsmodels
import statsmodels.api as sm

_LOG = logging.getLogger(__name__)


# #############################################################################
# Descriptive statistics
# #############################################################################


# TODO(Paul): Double-check axes in used in calculation.
def compute_moments(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate, mean, standard deviation, skew, and kurtosis.

    :param srs: input series for computing moments
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series of computed moments
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = replace_infs_with_nans(srs)
    data = hdf.apply_nan_mode(data, mode=nan_mode)
    result_index = [
        prefix + "mean",
        prefix + "std",
        prefix + "skew",
        prefix + "kurtosis",
    ]
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        nan_result = pd.Series(np.nan, index=result_index, name=srs.name)
        return nan_result
    # TODO(*): re-applyg stricter sp.stats nan_policy after we handle inf's.
    result_values = [
        data.mean(),
        data.std(),
        sp.stats.skew(data, nan_policy="omit"),
        sp.stats.kurtosis(data, nan_policy="omit"),
    ]
    result = pd.Series(data=result_values, index=result_index, name=srs.name)
    return result


# TODO(*): Move this function out of this library.
def replace_infs_with_nans(
    data: Union[pd.Series, pd.DataFrame],
) -> Union[pd.Series, pd.DataFrame]:
    """Replace infs with nans in a copy of `data`."""
    if data.empty:
        _LOG.warning("Empty input!")
    return data.replace([np.inf, -np.inf], np.nan)


def compute_frac_zero(
    data: Union[pd.Series, pd.DataFrame],
    atol: float = 0.0,
    axis: Optional[int] = 0,
) -> Union[float, pd.Series]:
    """Calculate fraction of zeros in a numerical series or dataframe.

    :param data: numeric series or dataframe
    :param atol: absolute tolerance, as in `np.isclose`
    :param axis: numpy axis for summation
    """
    # Create an ndarray of zeros of the same shape.
    zeros = np.zeros(data.shape)
    # Compare values of `df` to `zeros`.
    is_close_to_zero = np.isclose(data.values, zeros, atol=atol)
    num_zeros = is_close_to_zero.sum(axis=axis)
    return _compute_denominator_and_package(num_zeros, data, axis)


def compute_frac_nan(
    data: Union[pd.Series, pd.DataFrame], axis: Optional[int] = 0
) -> Union[float, pd.Series]:
    """Calculate fraction of nans in `data`.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_nans = data.isna().values.sum(axis=axis)
    return _compute_denominator_and_package(num_nans, data, axis)


def compute_frac_inf(
    data: Union[pd.Series, pd.DataFrame], axis: Optional[int] = 0
) -> Union[float, pd.Series]:
    """Count fraction of infs in a numerical series or dataframe.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_infs = np.isinf(data.values).sum(axis=axis)
    return _compute_denominator_and_package(num_infs, data, axis)


# TODO(Paul): Refactor to work with dataframes as well. Consider how to handle
#     `axis`, which the pd.Series version of `copy()` does not take.
def count_num_finite_samples(data: pd.Series) -> Union[int, float]:
    """Count number of finite data points in a given time series.

    :param data: numeric series or dataframe
    """
    if data.empty:
        _LOG.warning("Empty input series `%s`", data.name)
        return np.nan
    data = data.copy()
    data = replace_infs_with_nans(data)
    return data.count()


# TODO(Paul): Extend to dataframes.
def count_num_unique_values(data: pd.Series) -> Union[int, float]:
    """Count number of unique values in the series."""
    if data.empty:
        _LOG.warning("Empty input series `%s`", data.name)
        return np.nan
    srs = pd.Series(data=data.unique())
    return count_num_finite_samples(srs)


def _compute_denominator_and_package(
    reduction: Union[float, np.ndarray],
    data: Union[pd.Series, pd.DataFrame],
    axis: Optional[float] = None,
) -> Union[float, pd.Series]:
    """Normalize and package `reduction` according to `axis` and `data`
    metadata.

    This is a helper function used for several `compute_frac_*` functions:
    - It determines the denominator to use in normalization (for the `frac`
      part)
    - It packages the output so that it has index/column information as
      appropriate

    :param reduction: contains a reduction of `data` along `axis`
    :param data: numeric series or dataframe
    :param axis: indicates row or column or else `None` for ignoring 2d
        structure
    """
    if isinstance(data, pd.Series):
        df = data.to_frame()
    else:
        df = data
    nrows, ncols = df.shape
    # Ensure that there is data available.
    # TODO(Paul): Consider adding a check on the column data type.
    if nrows == 0 or ncols == 0:
        _LOG.warning("No data available!")
        return np.nan
    # Determine the correct denominator based on `axis`.
    if axis is None:
        denom = nrows * ncols
    elif axis == 0:
        denom = nrows
    elif axis == 1:
        denom = ncols
    else:
        raise ValueError("axis=%i" % axis)
    normalized = reduction / denom
    # Return float or pd.Series as appropriate based on dimensions and axis.
    if isinstance(normalized, float):
        dbg.dassert(not axis)
        return normalized
    dbg.dassert_isinstance(normalized, np.ndarray)
    if axis == 0:
        return pd.Series(data=normalized, index=df.columns)
    if axis == 1:
        return pd.Series(data=normalized, index=df.index)
    raise ValueError("axis=`%s` but expected to be `0` or `1`!" % axis)


def compute_special_value_stats(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate special value statistics in time series.

    :param srs: pandas series of floats
    :param prefix: optional prefix for metrics' outcome
    :return: series of statistics
    """
    prefix = prefix or ""
    dbg.dassert_isinstance(srs, pd.Series)
    result_index = [
        prefix + "n_rows",
        prefix + "frac_zero",
        prefix + "frac_nan",
        prefix + "frac_inf",
        prefix + "frac_constant",
        prefix + "num_finite_samples",
        prefix + "num_unique_values",
    ]
    nan_result = pd.Series(np.nan, index=result_index, name=srs.name)
    if srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    result_values = [
        len(srs),
        compute_frac_zero(srs),
        compute_frac_nan(srs),
        compute_frac_inf(srs),
        compute_zero_diff_proportion(srs).iloc[1],
        count_num_finite_samples(srs),
        count_num_unique_values(srs),
    ]
    result = pd.Series(data=result_values, index=result_index, name=srs.name)
    return result


# #############################################################################
# Sharpe ratio
# #############################################################################


def summarize_sharpe_ratio(
    log_rets: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate SR, SE(SR) from rets with an index freq and annualize.

    TODO(*): Consider de-biasing when the number of sample points is small,
        e.g., https://www.twosigma.com/wp-content/uploads/sharpe-tr-1.pdf
    """
    prefix = prefix or ""
    sr = compute_annualized_sharpe_ratio(log_rets)
    sr_se_estimate = compute_annualized_sharpe_ratio_standard_error(log_rets)
    res = pd.Series(
        data=[sr, sr_se_estimate],
        index=[prefix + "sharpe_ratio", prefix + "sharpe_ratio_standard_error"],
        name=log_rets.name,
    )
    return res


def zscore_oos_sharpe_ratio(
    log_rets: pd.Series, oos: Any, prefix: Optional[str] = None
) -> pd.Series:
    """Z-score the observed OOS SR wrt the INS SR and inflated SE.

    Calculate the following stats:
      - SR, SE(SR) for INS
      - SR, SE(SR) for OOS
      - z-scored OOS SR

    TODO(*): Consider factoring out pieces and/or returning more info.

    :param log_rets: log returns over entire period
    :param oos: start of OOS (right endpoint)
    :param prefix: prefix for the output series index
    :return: series of SR stats
    """
    prefix = prefix or ""
    # Create ins/oos masks.
    ins_mask = log_rets.index < oos
    dbg.dassert(ins_mask.any())
    ins_nobs = ins_mask.sum()
    oos_mask = log_rets.index >= oos
    dbg.dassert(oos_mask.any())
    oos_nobs = oos_mask.sum()
    #
    inflation = compute_sharpe_ratio_prediction_interval_inflation_factor(
        ins_nobs, oos_nobs
    )
    #
    ins_srs = log_rets.loc[ins_mask].copy()
    oos_srs = log_rets.loc[oos_mask].copy()
    # Compute INS Sharpe ratio and SE.
    ins_sr_and_se = summarize_sharpe_ratio(ins_srs, prefix="INS_")
    # Compute OOS Sharpe ratio and SE.
    oos_sr_and_se = summarize_sharpe_ratio(oos_srs, prefix="OOS_")
    # Z-score OOS SR using INS SR and inflated SE.
    pred_sr_se = inflation * ins_sr_and_se.loc["INS_sharpe_ratio_standard_error"]
    zscored_oos_sr = (
        oos_sr_and_se.loc["OOS_sharpe_ratio"]
        - ins_sr_and_se.loc["INS_sharpe_ratio"]
    ) / pred_sr_se
    # Combine results.
    zscored_oos_sr_srs = pd.Series(
        [zscored_oos_sr],
        name=oos_sr_and_se.name,
        index=["zscored_OOS_sharpe_ratio"],
    )
    res = pd.concat([ins_sr_and_se, oos_sr_and_se, zscored_oos_sr_srs])
    res.index = prefix + res.index
    return res


def compute_annualized_sharpe_ratio(
    log_rets: Union[pd.Series, pd.DataFrame],
) -> Union[float, pd.Series]:
    """Compute SR from rets with an index freq and annualize.

    :param log_rets: time series of log returns
    :return: annualized Sharpe ratio
    """
    points_per_year = hdf.infer_sampling_points_per_year(log_rets)
    if isinstance(log_rets, pd.Series):
        log_rets = hdf.apply_nan_mode(log_rets, mode="fill_with_zero")
    if isinstance(log_rets, pd.DataFrame):
        log_rets = log_rets.apply(hdf.apply_nan_mode, mode="fill_with_zero")
    sr = compute_sharpe_ratio(log_rets, points_per_year)
    return sr


def compute_annualized_sharpe_ratio_standard_error(
    log_rets: Union[pd.Series, pd.DataFrame],
) -> Union[float, pd.Series]:
    """Compute SE(SR) from rets with an index freq and annualize.

    This function calculates the standard error with respect to the original
    sampling frequency and then rescales to turn it into a standard error
    for the corresponding annualized Sharpe ratio.

    :param log_rets: time series of log returns
    :return: standard error estimate of annualized Sharpe ratio
    """
    points_per_year = hdf.infer_sampling_points_per_year(log_rets)
    log_rets = hdf.apply_nan_mode(log_rets, mode="fill_with_zero")
    se_sr = compute_sharpe_ratio_standard_error(log_rets, points_per_year)
    return se_sr


def compute_sharpe_ratio(
    log_rets: Union[pd.Series, pd.DataFrame], time_scaling: Union[int, float] = 1
) -> Union[float, pd.Series]:
    r"""Calculate Sharpe Ratio (SR) from log returns and rescale.

    For a detailed exploration of SR, see
    http://www.gilgamath.com/pages/ssc.html.

    :param log_rets: time series of log returns
    :param time_scaling: rescales SR by a factor of \sqrt(time_scaling).
        - For SR with respect to the sampling frequency, set equal to 1
        - For annualization, set equal to the number of sampling frequency
          ticks per year (e.g., =252 if daily returns are provided)
    :return: Sharpe Ratio
    """
    dbg.dassert_lte(1, time_scaling, f"time_scaling=`{time_scaling}`")
    sr = log_rets.mean() / log_rets.std()
    sr *= np.sqrt(time_scaling)
    if isinstance(sr, pd.Series):
        sr.name = "SR"
    return sr


def compute_sharpe_ratio_standard_error(
    log_rets: Union[pd.Series, pd.DataFrame], time_scaling: Union[int, float] = 1
) -> Union[float, pd.Series]:
    """Calculate Sharpe Ratio standard error from log returns and rescale.

    :param log_rets: time series of log returns
    :param time_scaling: as in `compute_sharpe_ratio`
    :return: Sharpe ratio standard error estimate
    """
    dbg.dassert_lte(1, time_scaling, f"time_scaling=`{time_scaling}`")
    # Compute the Sharpe ratio using the sampling frequency units[
    sr = compute_sharpe_ratio(log_rets, time_scaling=1)
    srs_size = hdf.apply_nan_mode(log_rets, mode="drop").size
    dbg.dassert_lt(1, srs_size)
    sr_var_estimate = (1 + (sr ** 2) / 2) / (srs_size - 1)
    sr_se_estimate = np.sqrt(sr_var_estimate)
    # Rescale.
    rescaled_sr_se_estimate = np.sqrt(time_scaling) * sr_se_estimate
    if isinstance(sr, pd.Series):
        rescaled_sr_se_estimate = "SE(SR)"
    return rescaled_sr_se_estimate


# #############################################################################
# Sharpe ratio tests
# #############################################################################


def apply_ttest_power_rule(
    alpha: float,
    power: float,
    two_sided: bool = False,
    years: Optional[float] = None,
    sharpe_ratio: Optional[float] = None,
) -> pd.Series:
    """Apply t-test power rule to SR will null hypothesis SR = 0.

    - The `power` is with respect to a specific type I error probability
      `alpha`
    - Supply exactly one of `years` and `sharpe_ratio`; the other is computed
      - E.g., if `sharpe_ratio` is supplied, then `years` can be interpreted
        as the required sample size
      - If `years` is supplied, then `sharpe_ratio` is the threshold at which
        the specified power holds.

    :param alpha: type I error probability
    :param power: 1 - type II error probability
    :param two_sided: one or two-sided t-test
    :param years: number of years
    :param sharpe_ratio: annualized Sharpe ratio
    :return: pd.Series of power rule formula values and assumptions
    """
    const = compute_ttest_power_rule_constant(
        alpha=alpha, power=power, two_sided=two_sided
    )
    if years is None and sharpe_ratio is not None:
        dbg.dassert_isinstance(sharpe_ratio, numbers.Number)
        years = const / (sharpe_ratio ** 2)
    elif years is not None and sharpe_ratio is None:
        dbg.dassert_isinstance(years, numbers.Number)
        sharpe_ratio = np.sqrt(const / years)
    else:
        raise ValueError(
            "Precisely one of `years` and `sharpe_ratio` should not be `None`"
        )
    idx = [
        "sharpe_ratio",
        "years",
        "power_rule_const",
        "type_I_error",
        "two_sided",
        "type_II_error",
        "power",
    ]
    data = [sharpe_ratio, years, const, alpha, two_sided, 1 - power, power]
    srs = pd.Series(index=idx, data=data, name="ttest_power_rule")
    return srs


def compute_ttest_power_rule_constant(
    alpha: float, power: float, two_sided: bool = False
) -> float:
    """Compute the constant to use in the t-test power law.

    E.g., http://www.vanbelle.org/chapters/webchapter2.pdf

    A special case of this is known as Lehr's rule.

    :param alpha: type I error rate
    :param power: 1 - type II error rate
    :param two_sided: one or two-sided t-test
    :return: constant to use in (one sample) t-test power law
    """
    dbg.dassert_lt(0, alpha)
    dbg.dassert_lt(alpha, 1)
    dbg.dassert_lt(0, power)
    dbg.dassert_lt(power, 1)
    if two_sided:
        alpha /= 2
    const: float = (sp.stats.norm.ppf(1 - alpha) + sp.stats.norm.ppf(power)) ** 2
    return const


def compute_sharpe_ratio_prediction_interval_inflation_factor(
    ins_nobs: Union[int, float], oos_nobs: Union[int, float]
) -> float:
    """Compute the SE(SR) inflation factor for obtaining conditional OOS
    bounds.

    :param ins_nobs: number of observations in-sample
    :param oos_nobs: number of observations out-of-sample
    :return: float > 1
    """
    se_inflation_factor = np.sqrt(1 + ins_nobs / oos_nobs)
    return se_inflation_factor


def compute_drawdown_cdf(
    sharpe_ratio: float,
    volatility: float,
    drawdown: float,
    time: Optional[float] = None,
) -> float:
    """Compute the drawdown cdf for `drawdown` at `time` given SR, vol specs.

    - Refs:
      - https://www.jstor.org/stable/3318509
      - https://en.wikipedia.org/wiki/Reflected_Brownian_motion
    - DD has law like that of RBM(-mu, sigma ** 2)
    - RMB(-mu, sigma ** 2) converges in distribution as t -> infinity to an
      exponential distribution with parameter 2 * mu / (sigma ** 2)
    - The drawdown cdf for the asymptotic distribution can be expressed in a
      "scale-free" way via an exponential distribution with parameter 2 * SR
      provided we interpret the result as being expressed in units of
      volatility (see `compute_normalized_drawdown_cdf()`).

    NOTE: The maximum drawdown experienced up to time `time` may exceed any
        terminal drawdown.

    :param sharpe_ratio: Sharpe ratio
    :param volatility: volatility, with units compatible with those of the
        Sharpe ratio
    :param drawdown: drawdown as a positive ratio
    :param time: time in units consistent with those of SR and vol (i.e.,
        "years" if SR and vol are annualized)
    :return: Prob(drawdown at time `time` <= `drawdown`)
        - If time is `None`, then we use the limiting exponential distribution.
    """
    dbg.dassert_lt(0, sharpe_ratio)
    dbg.dassert_lt(0, volatility)
    dbg.dassert_lt(0, drawdown)
    dbg.dassert_lt(0, time)
    normalized_drawdown = drawdown / volatility
    probability = compute_normalized_drawdown_cdf(
        sharpe_ratio=sharpe_ratio,
        normalized_drawdown=normalized_drawdown,
        time=time,
    )
    return probability


def compute_normalized_drawdown_cdf(
    sharpe_ratio: float,
    normalized_drawdown: float,
    time: Optional[float] = None,
) -> float:
    """Compute the drawdown cdf for drawdown given in units of volatility.

    :param sharpe_ratio: Sharpe ratio
    :param normalized_drawdown: drawdown in units of volatility, e.g.,
        (actual) drawdown / volatility
    :param time: time in units consistent with those of SR
    :return: Prob(normalized drawdown at time `time` <= `normalized_drawdown`)
    """
    dbg.dassert_lt(0, sharpe_ratio)
    dbg.dassert_lt(0, normalized_drawdown)
    dbg.dassert_lt(0, time)
    if time is None:
        a = np.inf
        b = np.inf
    else:
        # NOTE: SR and DD become unitless after these time multiplications.
        sr_mult_root_t = sharpe_ratio * np.sqrt(time)
        dd_div_root_t = normalized_drawdown / np.sqrt(time)
        a = sr_mult_root_t + dd_div_root_t
        b = sr_mult_root_t - dd_div_root_t
    probability: float = sp.stats.norm.cdf(a) - np.exp(
        -2 * sharpe_ratio * normalized_drawdown
    ) * sp.stats.norm.cdf(b)
    return probability


def compute_max_drawdown_approximate_cdf(
    sharpe_ratio: float, volatility: float, max_drawdown: float, time: float
) -> float:
    """Compute the approximate cdf for the maximum drawdown over a span of
    time.

    - https://www.sciencedirect.com/science/article/pii/S0304414913001695
    - G. F. Newell, Asymptotic Extreme Value Distribution for One-dimensional
      Diffusion Processes

    TODO(*): Revisit units and rescaling.
    TODO(*): Maybe normalize drawdown.

    :return: estimate of
        Prob(max drawdown over time period of length `time` <= `max_drawdown`)
    """
    dbg.dassert_lt(0, sharpe_ratio)
    dbg.dassert_lt(0, volatility)
    dbg.dassert_lt(0, max_drawdown)
    dbg.dassert_lt(0, time)
    lambda_ = 2 * sharpe_ratio / volatility
    # lambda_ * max_drawdown is the same as
    #     -2 * sharpe_ratio * (max_drawdown / volatility)
    y = lambda_ * max_drawdown - np.log(time)
    probability: float = sp.stats.gumbel_r.cdf(y)
    return probability


# #############################################################################
# Returns
# #############################################################################


def compute_annualized_return_and_volatility(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Annualized mean return and sample volatility in %.

    :param srs: series with datetimeindex with `freq`
    :param prefix: optional prefix for metrics' outcome
    :return: annualized pd.Series with return and volatility in %; pct rets
        if `srs` consists of pct rets, log rets if `srs` consists of log rets.
    """
    dbg.dassert_isinstance(srs, pd.Series)
    prefix = prefix or ""
    result_index = [
        prefix + "annualized_mean_return_(%)",
        prefix + "annualized_volatility_(%)",
    ]
    nan_result = pd.Series(
        np.nan, index=result_index, name=srs.name, dtype="float64"
    )
    if srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    annualized_mean_return = 100 * fin.compute_annualized_return(srs)
    annualized_volatility = 100 * fin.compute_annualized_volatility(srs)
    result = pd.Series(
        data=[annualized_mean_return, annualized_volatility],
        index=result_index,
        name=srs.name,
    )
    return result


def compute_max_drawdown(
    log_rets: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate max drawdown statistic.

    :param log_rets: pandas series of log returns
    :param prefix: optional prefix for metrics' outcome
    :return: max drawdown as a negative percentage loss
    """
    dbg.dassert_isinstance(log_rets, pd.Series)
    prefix = prefix or ""
    result_index = [prefix + "max_drawdown_(%)"]
    nan_result = pd.Series(
        index=result_index, name=log_rets.name, dtype="float64"
    )
    if log_rets.empty:
        _LOG.warning("Empty input series `%s`", log_rets.name)
        return nan_result
    pct_drawdown = fin.compute_perc_loss_from_high_water_mark(log_rets)
    max_drawdown = -100 * (pct_drawdown.max())
    result = pd.Series(data=max_drawdown, index=result_index, name=log_rets.name)
    return result


def compute_bet_stats(
    positions: pd.Series,
    log_rets: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate average returns for grouped bets.

    :param positions: series of long/short positions
    :param log_rets: log returns
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series of average returns for winning/losing and long/short bets,
        number of positions and bets. In `average_num_bets_per_year`, "year" is
        not the calendar year, but an approximate number of data points in a
        year
    """
    prefix = prefix or ""
    bet_lengths = fin.compute_signed_bet_lengths(positions, nan_mode=nan_mode)
    log_rets_per_bet = fin.compute_returns_per_bet(
        positions, log_rets, nan_mode=nan_mode
    )
    #
    stats = dict()
    stats["num_positions"] = bet_lengths.abs().sum()
    stats["num_bets"] = bet_lengths.size
    stats["long_bets_(%)"] = 100 * (bet_lengths > 0).sum() / bet_lengths.size
    n_years = positions.size / hdf.infer_sampling_points_per_year(positions)
    stats["avg_num_bets_per_year"] = bet_lengths.size / n_years
    # Format index.freq outcome to the word that represents its frequency.
    #    E.g. if `srs.index.freq` is equal to `<MonthEnd>` then
    #    this line will convert it to the string "Month".
    freq = str(positions.index.freq)[1:-1].split("End")[0]
    stats["avg_bet_length"] = bet_lengths.abs().mean()
    stats["bet_length_units"] = freq
    bet_hit_rate = calculate_hit_rate(log_rets_per_bet, prefix="bet_")
    stats.update(bet_hit_rate)
    #
    avg_ret_winning_bets = log_rets_per_bet.loc[log_rets_per_bet > 0].mean()
    stats["avg_return_winning_bets_(%)"] = 100 * fin.convert_log_rets_to_pct_rets(
        avg_ret_winning_bets
    )
    avg_ret_losing_bets = log_rets_per_bet.loc[log_rets_per_bet < 0].mean()
    stats["avg_return_losing_bets_(%)"] = 100 * fin.convert_log_rets_to_pct_rets(
        avg_ret_losing_bets
    )
    avg_ret_long_bet = log_rets_per_bet.loc[bet_lengths > 0].mean()
    stats["avg_return_long_bet_(%)"] = 100 * fin.convert_log_rets_to_pct_rets(
        avg_ret_long_bet
    )
    avg_ret_short_bet = log_rets_per_bet.loc[bet_lengths < 0].mean()
    stats["avg_return_short_bet_(%)"] = 100 * fin.convert_log_rets_to_pct_rets(
        avg_ret_short_bet
    )
    #
    srs = pd.Series(stats, name=log_rets.name)
    srs.index = prefix + srs.index
    return srs


def calculate_hit_rate(
    srs: pd.Series,
    alpha: Optional[float] = None,
    method: Optional[str] = None,
    threshold: Optional[float] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate hit rate statistics.

    :param srs: pandas series
    :param alpha: as in statsmodels.stats.proportion.proportion_confint()
    :param method: as in statsmodels.stats.proportion.proportion_confint()
    :param threshold: threshold value around zero to exclude from calculations
    :param prefix: optional prefix for metrics' outcome
    :return: hit rate statistics: point estimate, lower bound, upper bound
    """
    alpha = alpha or 0.05
    method = method or "jeffreys"
    dbg.dassert_lte(0, alpha)
    dbg.dassert_lte(alpha, 1)
    dbg.dassert_isinstance(srs, pd.Series)
    threshold = threshold or 0
    dbg.dassert_lte(0, threshold)
    prefix = prefix or ""
    # Process series.
    conf_alpha = (1 - alpha / 2) * 100
    result_index = [
        prefix + "hit_rate_point_est_(%)",
        prefix + f"hit_rate_{conf_alpha:.2f}%CI_lower_bound_(%)",
        prefix + f"hit_rate_{conf_alpha:.2f}%CI_upper_bound_(%)",
    ]
    # Set all the values whose absolute values are closer to zero than
    #    the absolute value of the threshold equal to NaN.
    srs = srs.mask(abs(srs) < threshold)
    # Set all the inf values equal to NaN.
    srs = srs.replace([np.inf, -np.inf, 0], np.nan)
    # Drop all the NaN values.
    srs = hdf.apply_nan_mode(srs, mode="drop")
    if srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        nan_result = pd.Series(index=result_index, name=srs.name, dtype="float64")
        return nan_result
    hit_mask = srs >= threshold
    # Calculate confidence intervals.
    point_estimate = hit_mask.mean()
    hit_lower, hit_upper = statsmodels.stats.proportion.proportion_confint(
        count=hit_mask.sum(), nobs=hit_mask.count(), alpha=alpha, method=method
    )
    result_values_pct = [100 * point_estimate, 100 * hit_lower, 100 * hit_upper]
    result = pd.Series(data=result_values_pct, index=result_index, name=srs.name)
    return result


# #############################################################################
# Hypothesis testing
# #############################################################################


def ttest_1samp(
    srs: pd.Series,
    popmean: Optional[float] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Thin wrapper around scipy's ttest.

    :param srs: input series for computing statistics
    :param popmean: assumed population mean for test
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series with t-value and p-value
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    popmean = popmean or 0
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    result_index = [
        prefix + "tval",
        prefix + "pval",
    ]
    nan_result = pd.Series(data=np.nan, index=result_index, name=srs.name)
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    try:
        tval, pval = sp.stats.ttest_1samp(
            data, popmean=popmean, nan_policy="raise"
        )
    except ValueError as inst:
        _LOG.warning(inst)
        return nan_result
    result_values = [
        tval,
        pval,
    ]
    result = pd.Series(data=result_values, index=result_index, name=data.name)
    return result


def multipletests(
    srs: pd.Series,
    method: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Wrap statsmodel's multipletests.

    Returns results in a series indexed like srs.
    Documentation at
    https://www.statsmodels.org/stable/generated/statsmodels.stats.multitest.multipletests.html

    :param srs: Series with pvalues
    :param method: `method` for scipy's multipletests
    :param nan_mode: approach to deal with NaNs, can be "strict" or "drop"
    :param prefix: optional prefix for metrics' outcome
    :return: Series of adjusted p-values
    """
    dbg.dassert_isinstance(srs, pd.Series)
    method = method or "fdr_bh"
    nan_mode = nan_mode or "strict"
    dbg.dassert_in(nan_mode, ["strict", "drop"])
    prefix = prefix or ""
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    if data.empty:
        _LOG.warning("Empty input series `%s`", data.name)
        return pd.Series([np.nan], name=prefix + "adj_pval")
    pvals_corrected = statsmodels.stats.multitest.multipletests(
        data, method=method
    )[1]
    return pd.Series(pvals_corrected, index=data.index, name=prefix + "adj_pval")


def multi_ttest(
    data: pd.DataFrame,
    popmean: Optional[float] = None,
    nan_mode: Optional[str] = None,
    method: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """Combine ttest and multitest pvalue adjustment."""
    popmean = popmean or 0
    nan_mode = nan_mode or "drop"
    method = method or "fdr_bh"
    prefix = prefix or ""
    dbg.dassert_isinstance(data, pd.DataFrame)
    if data.empty:
        _LOG.warning("Empty input!")
        return pd.DataFrame(
            np.nan,
            index=[prefix + "tval", prefix + "pval", prefix + "adj_pval"],
            columns=[data.columns],
        )
    res = data.apply(
        ttest_1samp, popmean=popmean, nan_mode=nan_mode, prefix=prefix
    ).T
    res[prefix + "adj_pval"] = multipletests(res[prefix + "pval"], method=method)
    return res


def apply_normality_test(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Test (indep) null hypotheses that each col is normally distributed.

    An omnibus test of normality that combines skew and kurtosis.

    :param prefix: optional prefix for metrics' outcome
    :param nan_mode: argument for hdf.apply_nan_mode()
    :return: series with statistics and p-value
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    result_index = [
        prefix + "stat",
        prefix + "pval",
    ]
    nan_result = pd.Series(data=np.nan, index=result_index, name=srs.name)
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    try:
        stat, pval = sp.stats.normaltest(data, nan_policy="raise")
    except ValueError as inst:
        # This can raise if there are not enough data points, but the number
        # required can depend upon the input parameters.
        _LOG.warning(inst)
        return nan_result
    result_values = [
        stat,
        pval,
    ]
    result = pd.Series(data=result_values, index=result_index, name=data.name)
    return result


# TODO(*): Maybe add `inf_mode`.
def apply_adf_test(
    srs: pd.Series,
    maxlag: Optional[int] = None,
    regression: Optional[str] = None,
    autolag: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Implement a wrapper around statsmodels' adfuller test.

    :param srs: pandas series of floats
    :param maxlag: as in stattools.adfuller
    :param regression: as in stattools.adfuller
    :param autolag: as in stattools.adfuller
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue, and related info
    """
    dbg.dassert_isinstance(srs, pd.Series)
    regression = regression or "c"
    autolag = autolag or "AIC"
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    # Hack until we factor out inf handling.
    srs = srs.replace([np.inf, -np.inf], np.nan)
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    # https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.adfuller.html
    result_index = [
        prefix + "stat",
        prefix + "pval",
        prefix + "used_lag",
        prefix + "nobs",
        prefix + "critical_values_1%",
        prefix + "critical_values_5%",
        prefix + "critical_values_10%",
        prefix + "ic_best",
    ]
    nan_result = pd.Series(
        data=np.nan,
        index=result_index,
        name=data.name,
    )
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    try:
        # The output of sm.tsa.stattools.adfuller in this case can only be
        # of the length 6 so ignore the lint.
        (
            adf_stat,
            pval,
            usedlag,
            nobs,
            critical_values,
            icbest,
        ) = sm.tsa.stattools.adfuller(
            data.values, maxlag=maxlag, regression=regression, autolag=autolag
        )
    except ValueError as inst:
        # This can raise if there are not enough data points, but the number
        # required can depend upon the input parameters.
        _LOG.warning(inst)
        return nan_result
        #
    result_values = [
        adf_stat,
        pval,
        usedlag,
        nobs,
        critical_values["1%"],
        critical_values["5%"],
        critical_values["10%"],
        icbest,
    ]
    result = pd.Series(data=result_values, index=result_index, name=data.name)
    return result


def apply_kpss_test(
    srs: pd.Series,
    regression: Optional[str] = None,
    nlags: Optional[Union[int, str]] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Implement a wrapper around statsmodels' KPSS test.

    http://debis.deu.edu.tr/userweb//onder.hanedar/dosyalar/kpss.pdf

    :param srs: pandas series of floats
    :param regression: as in stattools.kpss
    :param nlags: as in stattools.kpss
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue, and related info
    """
    dbg.dassert_isinstance(srs, pd.Series)
    regression = regression or "c"
    nan_mode = nan_mode or "drop"
    nlags = nlags or "auto"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    # https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.kpss.html
    result_index = [
        prefix + "stat",
        prefix + "pval",
        prefix + "lags",
        prefix + "critical_values_1%",
        prefix + "critical_values_5%",
        prefix + "critical_values_10%",
    ]
    nan_result = pd.Series(
        data=np.nan,
        index=result_index,
        name=data.name,
    )
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    try:
        (
            kpss_stat,
            pval,
            lags,
            critical_values,
        ) = sm.tsa.stattools.kpss(data.values, regression=regression, nlags=nlags)
    except (ValueError, OverflowError):
        # This can raise if there are not enough data points, but the number
        # required can depend upon the input parameters.
        # TODO(Julia): Debug OverflowError.
        return nan_result
        #
    result_values = [
        kpss_stat,
        pval,
        lags,
        critical_values["1%"],
        critical_values["5%"],
        critical_values["10%"],
    ]
    result = pd.Series(data=result_values, index=result_index, name=data.name)
    return result


def apply_ljung_box_test(
    srs: pd.Series,
    lags: Optional[Union[int, pd.Series]] = None,
    model_df: Optional[int] = None,
    period: Optional[int] = None,
    return_df: Optional[bool] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """Implement a wrapper around statsmodels' Ljung-Box test.

    :param srs: pandas series of floats
    :param lags: as in diagnostic.acorr_ljungbox
    :param model_df: as in diagnostic.acorr_ljungbox
    :param period: as in diagnostic.acorr_ljungbox
    :param return_df: as in diagnostic.acorr_ljungbox
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue
    """
    dbg.dassert_isinstance(srs, pd.Series)
    model_df = model_df or 0
    return_df = return_df or True
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    # https://www.statsmodels.org/stable/generated/statsmodels.stats.diagnostic.acorr_ljungbox.html
    columns = [
        prefix + "stat",
        prefix + "pval",
    ]
    # Make an output for empty or too short inputs.
    nan_result = pd.DataFrame([[np.nan, np.nan]], columns=columns)
    if data.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    try:
        result = sm.stats.diagnostic.acorr_ljungbox(
            data.values,
            lags=lags,
            model_df=model_df,
            period=period,
            return_df=return_df,
        )
    except ValueError as inst:
        _LOG.warning(inst)
        return nan_result
    #
    if return_df:
        df_result = result
    else:
        df_result = pd.DataFrame(result).T
    df_result.columns = columns
    return df_result


def compute_jensen_ratio(
    signal: pd.Series,
    p_norm: float = 2,
    inf_mode: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Calculate a ratio >= 1 with equality only when Jensen's inequality
    holds.

    Definition and derivation:
      - The result is the p-th root of the expectation of the p-th power of
        abs(f), divided by the expectation of abs(f). If we apply Jensen's
        inequality to (abs(signal)**p)**(1/p), renormalizing the lower bound to
        1, then the upper bound is the valued calculated by this function.
      - An alternative derivation is to apply Holder's inequality to `signal`,
        using the constant function `1` on the support of the `signal` as the
        2nd function.

    Interpretation:
      - If we apply this function to returns in the case where the expected
        value of returns is 0 and we take p_norm = 2, then the result of this
        function can be interpreted as a renormalized realized volatility.
      - For a Gaussian signal, the expected value is np.sqrt(np.pi / 2), which
        is approximately 1.25. This holds regardless of the volatility of the
        Gaussian (so the measure is scale invariant).
      - For a stationary function, the expected value does not change with
        sampled series length.
      - For a signal that is t-distributed with 4 dof, the expected value is
        approximately 1.41.
    """
    dbg.dassert_isinstance(signal, pd.Series)
    # Require that we evaluate a norm.
    dbg.dassert_lte(1, p_norm)
    # TODO(*): Maybe add l-infinity support. For many stochastic signals, we
    # should not expect a finite value in the continuous limit.
    dbg.dassert(np.isfinite(p_norm))
    # Set reasonable defaults for inf and nan modes.
    inf_mode = inf_mode or "drop"
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(signal, mode=nan_mode)
    nan_result = pd.Series(
        data=np.nan, index=[prefix + "jensen_ratio"], name=signal.name
    )
    dbg.dassert(not data.isna().any())
    # Handle infs.
    # TODO(*): apply special functions for inf_mode after #2624 is completed.
    has_infs = (~data.apply(np.isfinite)).any()
    if has_infs:
        if inf_mode == "return_nan":
            # According to a strict interpretation, each norm is infinite, and
            # and so their quotient is undefined.
            return nan_result
        if inf_mode == "drop":
            # Replace inf values with np.nan and drop.
            data = data.replace([-np.inf, np.inf], np.nan).dropna()
        else:
            raise ValueError(f"Unrecognized inf_mode `{inf_mode}")
    dbg.dassert(data.apply(np.isfinite).all())
    # Return NaN if there is no data.
    if data.size == 0:
        _LOG.warning("Empty input signal `%s`", signal.name)
        return nan_result
    # Calculate norms.
    lp = sp.linalg.norm(data, ord=p_norm)
    l1 = sp.linalg.norm(data, ord=1)
    # Ignore support where `signal` has NaNs.
    scaled_support = data.size ** (1 - 1 / p_norm)
    jensen_ratio = scaled_support * lp / l1
    res = pd.Series(
        data=[jensen_ratio], index=[prefix + "jensen_ratio"], name=signal.name
    )
    return res


def compute_forecastability(
    signal: pd.Series,
    mode: str = "welch",
    inf_mode: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    r"""Compute frequency-domain-based "forecastability" of signal.

    Reference: https://arxiv.org/abs/1205.4591

    `signal` is assumed to be second-order stationary.

    Denote the forecastability estimator by \Omega(\cdot).
    Let x_t, y_t be time series. Properties of \Omega include:
    a) \Omega(y_t) = 0 iff y_t is white noise
    b) scale and shift-invariant:
         \Omega(a y_t + b) = \Omega(y_t) for real a, b, a \neq 0.
    c) max sub-additivity for uncorrelated processes:
         \Omega(\alpha x_t + \sqrt{1 - \alpha^2} y_t) \leq
         \max\{\Omega(x_t), \Omega(y_t)\},
       if \E(x_t y_s) = 0 for all s, t \in \Z;
       equality iff alpha \in \{0, 1\}.
    """
    dbg.dassert_isinstance(signal, pd.Series)
    nan_mode = nan_mode or "fill_with_zero"
    inf_mode = inf_mode or "drop"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(signal, mode=nan_mode)
    has_infs = (~data.apply(np.isfinite)).any()
    if has_infs:
        if inf_mode == "drop":
            # Replace inf with np.nan and drop.
            data = data.replace([-np.inf, np.inf], np.nan).dropna()
        else:
            raise ValueError(f"Unrecognized inf_mode `{inf_mode}")
    dbg.dassert(data.apply(np.isfinite).all())
    # Return NaN if there is no data.
    if data.size == 0:
        _LOG.warning("Empty input signal `%s`", signal.name)
        nan_result = pd.Series(
            data=np.nan, index=[prefix + "forecastability"], name=signal.name
        )
        return nan_result
    if mode == "welch":
        _, psd = sp.signal.welch(data)
    elif mode == "periodogram":
        # TODO(Paul): Maybe log a warning about inconsistency of periodogram
        #     for estimating power spectral density.
        _, psd = sp.signal.periodogram(data)
    else:
        raise ValueError("Unsupported mode=`%s`" % mode)
    forecastability = 1 - sp.stats.entropy(psd, base=psd.size)
    res = pd.Series(
        data=[forecastability],
        index=[prefix + "forecastability"],
        name=signal.name,
    )
    return res


def compute_zero_diff_proportion(
    srs: pd.Series,
    atol: Optional[float] = None,
    rtol: Optional[float] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Compute proportion of unvarying periods in a series.

    https://numpy.org/doc/stable/reference/generated/numpy.isclose.html

    :param srs: pandas series of floats
    :param atol: as in numpy.isclose
    :param rtol: as in numpy.isclose
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series with proportion of unvarying periods
    """
    dbg.dassert_isinstance(srs, pd.Series)
    atol = atol or 0
    rtol = rtol or 1e-05
    nan_mode = nan_mode or "leave_unchanged"
    prefix = prefix or ""
    srs = srs.replace([np.inf, -np.inf], np.nan)
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    result_index = [
        prefix + "approx_const_count",
        prefix + "approx_const_frac",
    ]
    if data.shape[0] < 2:
        _LOG.warning(
            "Input series `%s` with size '%d' is too small",
            srs.name,
            data.shape[0],
        )
        nan_result = pd.Series(data=np.nan, index=result_index, name=srs.name)
        return nan_result
    # Compute if neighboring elements are equal within the given tolerance.
    equal_ngb_srs = np.isclose(data.shift(1)[1:], data[1:], atol=atol, rtol=rtol)
    # Compute number and proportion of equals among all neighbors pairs.
    approx_const_count = equal_ngb_srs.sum()
    n_pairs = data.shape[0] - 1
    approx_const_frac = approx_const_count / n_pairs
    result_values = [approx_const_count, approx_const_frac]
    res = pd.Series(data=result_values, index=result_index, name=srs.name)
    return res


def get_interarrival_time(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
) -> Optional[pd.Series]:
    """Get interrarival time from index of a time series.

    :param srs: pandas series of floats
    :param nan_mode: argument for hdf.apply_nan_mode()
    :return: series with interrarival time
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    if data.empty:
        _LOG.warning("Empty input `%s`", srs.name)
        return None
    index = data.index
    # Check index of a series. We require that the input
    #     series have a sorted datetime index.
    dbg.dassert_isinstance(index, pd.DatetimeIndex)
    dbg.dassert_strictly_increasing_index(index)
    # Compute a series of interrairival time.
    interrarival_time = pd.Series(index).diff()
    return interrarival_time


def compute_interarrival_time_stats(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Compute interarrival time statistics.

    :param srs: pandas series of interrarival time
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series with statistic and related info
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdf.apply_nan_mode(srs, mode=nan_mode)
    result_index = [
        prefix + "n_unique",
        prefix + "mean",
        prefix + "std",
        prefix + "min",
        prefix + "max",
    ]
    nan_result = pd.Series(index=result_index, name=data.name, dtype="object")
    if data.shape[0] < 2:
        _LOG.warning(
            "Input series `%s` with size '%d' is too small",
            srs.name,
            data.shape[0],
        )
        return nan_result
    interarrival_time = get_interarrival_time(data)
    if interarrival_time is None:
        return nan_result
    n_unique = interarrival_time.nunique()
    mean = interarrival_time.mean()
    std = interarrival_time.std()
    min_value = interarrival_time.min()
    max_value = interarrival_time.max()
    #
    result_values = [n_unique, mean, std, min_value, max_value]
    res = pd.Series(
        data=result_values, index=result_index, name=srs.name, dtype="object"
    )
    return res


def compute_avg_turnover_and_holding_period(
    pos: pd.Series,
    unit: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Compute average turnover and holding period for a sequence of positions.

    :param pos: pandas series of positions
    :param unit: desired output holding period unit (e.g. 'B', 'W', 'M', etc.)
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: average turnover, holding period and index frequency
    """
    dbg.dassert_isinstance(pos, pd.Series)
    dbg.dassert(pos.index.freq)
    pos_freq = pos.index.freq
    unit = unit or pos_freq
    nan_mode = nan_mode or "ffill"
    prefix = prefix or ""
    result_index = [
        prefix + "avg_turnover_(%)",
        prefix + "turnover_frequency",
        prefix + "avg_holding_period",
        prefix + "holding_period_units",
    ]
    avg_holding_period = fin.compute_average_holding_period(
        pos=pos, unit=unit, nan_mode=nan_mode
    )
    avg_turnover = 100 * (1 / avg_holding_period)
    #
    result_values = [avg_turnover, unit, avg_holding_period, unit]
    res = pd.Series(data=result_values, index=result_index, name=pos.name)
    return res


# #############################################################################
# Cross-validation
# #############################################################################


def get_rolling_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """Partition index into chunks and returns pairs of successive chunks.

    If the index looks like
        [0, 1, 2, 3, 4, 5, 6]
    and n_splits = 4, then the splits would be
        [([0, 1], [2, 3]),
         ([2, 3], [4, 5]),
         ([4, 5], [6])]

    A typical use case is where the index is a monotonic increasing datetime
    index. For such cases, causality is respected by the splits.
    """
    dbg.dassert_strictly_increasing_index(idx)
    n_chunks = n_splits + 1
    dbg.dassert_lte(1, n_splits)
    # Split into equal chunks.
    chunk_size = int(math.ceil(idx.size / n_chunks))
    dbg.dassert_lte(1, chunk_size)
    chunks = [idx[i : i + chunk_size] for i in range(0, idx.size, chunk_size)]
    dbg.dassert_eq(len(chunks), n_chunks)
    #
    splits = list(zip(chunks[:-1], chunks[1:]))
    return splits


def get_oos_start_split(
    idx: pd.Index, datetime_: Union[datetime.datetime, pd.Timestamp]
) -> List[Tuple[pd.Index, pd.Index]]:
    """Split index using OOS (out-of-sample) start datetime."""
    dbg.dassert_strictly_increasing_index(idx)
    ins_mask = idx < datetime_
    dbg.dassert_lte(1, ins_mask.sum())
    oos_mask = ~ins_mask
    dbg.dassert_lte(1, oos_mask.sum())
    ins = idx[ins_mask]
    oos = idx[oos_mask]
    return [(ins, oos)]


# TODO(Paul): Support train/test/validation or more.
def get_train_test_pct_split(
    idx: pd.Index, train_pct: float
) -> List[Tuple[pd.Index, pd.Index]]:
    """Split index into train and test sets by percentage."""
    dbg.dassert_strictly_increasing_index(idx)
    dbg.dassert_lt(0.0, train_pct)
    dbg.dassert_lt(train_pct, 1.0)
    #
    train_size = int(train_pct * idx.size)
    dbg.dassert_lte(0, train_size)
    train_split = idx[:train_size]
    test_split = idx[train_size:]
    return [(train_split, test_split)]


def get_expanding_window_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """Generate splits with expanding overlapping windows."""
    dbg.dassert_strictly_increasing_index(idx)
    dbg.dassert_lte(1, n_splits)
    tscv = sklearn.model_selection.TimeSeriesSplit(n_splits=n_splits)
    locs = list(tscv.split(idx))
    splits = [(idx[loc[0]], idx[loc[1]]) for loc in locs]
    return splits


def truncate_index(idx: pd.Index, min_idx: Any, max_idx: Any) -> pd.Index:
    """Return subset of idx with values >= min_idx and < max_idx."""
    dbg.dassert_strictly_increasing_index(idx)
    # TODO(*): PartTask667: Consider using bisection to avoid linear scans.
    min_mask = idx >= min_idx
    max_mask = idx < max_idx
    mask = min_mask & max_mask
    dbg.dassert_lte(1, mask.sum())
    return idx[mask]


def combine_indices(idxs: Iterable[pd.Index]) -> pd.Index:
    """Combine multiple indices into a single index for cross-validation
    splits.

    This is computed as the union of all the indices within the largest common
    interval.

    TODO(Paul): Consider supporting multiple behaviors with `mode`.
    """
    for idx in idxs:
        dbg.dassert_strictly_increasing_index(idx)
    # Find the maximum start/end datetime overlap of all source indices.
    max_min = max([idx.min() for idx in idxs])
    _LOG.debug("Latest start datetime of indices=%s", max_min)
    min_max = min([idx.max() for idx in idxs])
    _LOG.debug("Earliest end datetime of indices=%s", min_max)
    truncated_idxs = [truncate_index(idx, max_min, min_max) for idx in idxs]
    # Take the union of truncated indices. Though all indices fall within the
    # datetime range [max_min, min_max), they do not necessarily have the same
    # resolution or all values.
    composite_idx = functools.reduce(lambda x, y: x.union(y), truncated_idxs)
    return composite_idx


def convert_splits_to_string(splits: collections.OrderedDict) -> str:
    txt = "n_splits=%s\n" % len(splits)
    for train_idxs, test_idxs in splits:
        txt += "train=%s [%s, %s]" % (
            len(train_idxs),
            min(train_idxs),
            max(train_idxs),
        )
        txt += "\n"
        txt += "test=%s [%s, %s]" % (
            len(test_idxs),
            min(test_idxs),
            max(test_idxs),
        )
        txt += "\n"
    return txt


def summarize_time_index_info(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """Return summarized information about datetime index of the input.

    :param srs: pandas series of floats
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param prefix: optional prefix for output's index
    :return: series with information about input's index
    """
    dbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    original_index = srs.index
    # Assert that input series has a sorted datetime index.
    dbg.dassert_isinstance(original_index, pd.DatetimeIndex)
    dbg.dassert_strictly_increasing_index(original_index)
    freq = original_index.freq
    clear_srs = hdf.apply_nan_mode(srs, mode=nan_mode)
    clear_index = clear_srs.index
    result = pd.Series([], dtype="object")
    if clear_srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        result["start_time"] = np.nan
        result["end_time"] = np.nan
    else:
        result["start_time"] = clear_index[0]
        result["end_time"] = clear_index[-1]
    result["n_sampling_points"] = len(clear_index)
    result["frequency"] = freq
    if freq is None:
        sampling_points_per_year = clear_srs.resample("Y").count().mean()
    else:
        sampling_points_per_year = hdf.compute_points_per_year_for_given_freq(
            freq
        )
    result["sampling_points_per_year"] = sampling_points_per_year
    # Compute input time span as a number of `freq` units in
    # `clear_index`.
    if not clear_srs.empty:
        clear_index_time_span = len(srs[clear_index[0] : clear_index[-1]])
    else:
        clear_index_time_span = 0
    result["time_span_in_years"] = (
        clear_index_time_span / sampling_points_per_year
    )
    result.index = prefix + result.index
    return result
