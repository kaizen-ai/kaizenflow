"""
Import as:

import core.statistics as costatis
"""

import collections
import datetime
import functools
import logging
import math
import numbers
from typing import Any, Iterable, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
import scipy as sp
import sklearn.model_selection
import statsmodels
import statsmodels.api as sm

import core.finance as cofinanc
import core.signal_processing as csigproc
import helpers.dataframe as hdatafr
import helpers.dbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


# #############################################################################
# Sampling statistics: start, end, frequency, NaNs, infs, etc.
# #############################################################################


def summarize_time_index_info(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Return summarized information about datetime index of the input.

    :param srs: pandas series of floats
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for output's index
    :return: series with information about input's index
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    original_index = srs.index
    # Assert that input series has a sorted datetime index.
    hdbg.dassert_isinstance(original_index, pd.DatetimeIndex)
    hpandas.dassert_strictly_increasing_index(original_index)
    freq = original_index.freq
    clear_srs = hdatafr.apply_nan_mode(srs, mode=nan_mode)
    clear_index = clear_srs.index
    result = {}
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
        sampling_points_per_year = hdatafr.compute_points_per_year_for_given_freq(
            freq
        )
    result["sampling_points_per_year"] = sampling_points_per_year
    # Compute input time span as a number of `freq` units in
    # `clear_index`.
    if not clear_srs.empty:
        if freq is None:
            clear_index_time_span = (clear_index[-1] - clear_index[0]).days
            sampling_points_per_year = (
                hdatafr.compute_points_per_year_for_given_freq("D")
            )
        else:
            clear_index_time_span = len(srs[clear_index[0] : clear_index[-1]])
    else:
        clear_index_time_span = 0
    result["time_span_in_years"] = (
        clear_index_time_span / sampling_points_per_year
    )
    result = pd.Series(result, dtype="object")
    result.index = prefix + result.index
    return result


def compute_special_value_stats(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate special value statistics in time series.

    :param srs: pandas series of floats
    :param prefix: optional prefix for metrics' outcome
    :return: series of statistics
    """
    prefix = prefix or ""
    hdbg.dassert_isinstance(srs, pd.Series)
    result_index = [
        prefix + "n_rows",
        prefix + "frac_zero",
        prefix + "frac_nan",
        prefix + "frac_inf",
        prefix + "frac_constant",
        prefix + "num_finite_samples",
        prefix + "num_finite_samples_inv",
        prefix + "num_finite_samples_inv_dyadic_scale",
        prefix + "num_finite_samples_sqrt",
        prefix + "num_finite_samples_sqrt_inv",
        prefix + "num_finite_samples_sqrt_inv_dyadic_scale",
        prefix + "num_unique_values",
    ]
    nan_result = pd.Series(np.nan, index=result_index, name=srs.name)
    if srs.empty:
        _LOG.warning("Empty input series `%s`", srs.name)
        return nan_result
    num_finite_samples = count_num_finite_samples(srs)
    result_values = [
        len(srs),
        compute_frac_zero(srs),
        compute_frac_nan(srs),
        compute_frac_inf(srs),
        compute_zero_diff_proportion(srs).iloc[1],
        num_finite_samples,
        1 / num_finite_samples,
        compute_dyadic_scale(1 / num_finite_samples),
        np.sqrt(num_finite_samples),
        1 / np.sqrt(num_finite_samples),
        compute_dyadic_scale(1 / np.sqrt(num_finite_samples)),
        count_num_unique_values(srs),
    ]
    result = pd.Series(
        data=result_values, index=result_index, name=srs.name, dtype=object
    )
    return result


# TODO(*): Move this function out of this library.
def replace_infs_with_nans(
    data: Union[pd.Series, pd.DataFrame],
) -> Union[pd.Series, pd.DataFrame]:
    """
    Replace infs with nans in a copy of `data`.
    """
    if data.empty:
        _LOG.warning("Empty input!")
    return data.replace([np.inf, -np.inf], np.nan)


def compute_frac_zero(
    data: Union[pd.Series, pd.DataFrame],
    atol: float = 0.0,
    axis: Optional[int] = 0,
) -> Union[float, pd.Series]:
    """
    Calculate fraction of zeros in a numerical series or dataframe.

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
    """
    Calculate fraction of nans in `data`.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_nans = data.isna().values.sum(axis=axis)
    return _compute_denominator_and_package(num_nans, data, axis)


def compute_frac_inf(
    data: Union[pd.Series, pd.DataFrame], axis: Optional[int] = 0
) -> Union[float, pd.Series]:
    """
    Count fraction of infs in a numerical series or dataframe.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_infs = np.isinf(data.values).sum(axis=axis)
    return _compute_denominator_and_package(num_infs, data, axis)


# TODO(Paul): Refactor to work with dataframes as well. Consider how to handle
#     `axis`, which the pd.Series version of `copy()` does not take.
def count_num_finite_samples(data: pd.Series) -> Union[int, float]:
    """
    Count number of finite data points in a given time series.

    :param data: numeric series or dataframe
    """
    if data.empty:
        _LOG.warning("Empty input series `%s`", data.name)
        return np.nan  # type: ignore
    data = data.copy()
    data = replace_infs_with_nans(data)
    ret = data.count()
    ret = cast(int, ret)
    return ret


# TODO(Paul): Extend to dataframes.
def count_num_unique_values(data: pd.Series) -> Union[int, float, np.float]:
    """
    Count number of unique values in the series.
    """
    if data.empty:
        _LOG.warning("Empty input series `%s`", data.name)
        return np.nan
    srs = pd.Series(data=data.unique())
    return count_num_finite_samples(srs)


def compute_zero_diff_proportion(
    srs: pd.Series,
    atol: Optional[float] = None,
    rtol: Optional[float] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Compute proportion of unvarying periods in a series.

    https://numpy.org/doc/stable/reference/generated/numpy.isclose.html

    :param srs: pandas series of floats
    :param atol: as in numpy.isclose
    :param rtol: as in numpy.isclose
    :param nan_mode: argument for hdatafr.apply_nan_mode()
        If `nan_mode` is "leave_unchanged":
          - consecutive `NaN`s are not counted as a constant period
          - repeated values with `NaN` in between are not counted as a constant
            period
        If `nan_mode` is "drop":
          - the denominator is reduced by the number of `NaN` and `inf` values
          - repeated values with `NaN` in between are counted as a constant
            period
        If `nan_mode` is "ffill":
          - consecutive `NaN`s are counted as a constant period
          - repeated values with `NaN` in between are counted as a constant
            period
    :param prefix: optional prefix for metrics' outcome
    :return: series with proportion of unvarying periods
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    atol = atol or 0
    rtol = rtol or 1e-05
    nan_mode = nan_mode or "leave_unchanged"
    prefix = prefix or ""
    srs = srs.replace([np.inf, -np.inf], np.nan)
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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


def compute_dyadic_scale(num: float) -> int:
    """
    Return the dyadic scale of a number.

    We take this to be the integer `j` such that 2 ** j <= abs(num) < 2
    ** (j + 1)
    """
    abs_num = np.abs(num)
    hdbg.dassert_lt(0, abs_num)
    j = np.floor(np.log2(abs_num))
    return int(j)


# #############################################################################
# Summary statistics: location, spread, shape, diversity, cardinality, entropy
# #############################################################################


# TODO(Paul): Double-check axes in used in calculation.
def compute_moments(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate, mean, standard deviation, skew, and kurtosis.

    :param srs: input series for computing moments
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series of computed moments
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = replace_infs_with_nans(srs)
    data = hdatafr.apply_nan_mode(data, mode=nan_mode)
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


def compute_jensen_ratio(
    signal: pd.Series,
    p_norm: float = 2,
    inf_mode: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate a 0 < ratio <= 1 based on Jensen's inequality.

    TODO(Paul): Rename this "J-ratio" and the function `compute_j_ratio()` for
        short.

    Definition and derivation:
      - The result is the expectation of abs(f) divided by the p-th root of the
        expectation of the p-th power of abs(f). If we apply Jensen's
        inequality to (abs(signal)**p)**(1/p), renormalizing the greater value
        to 1, then the lesser value is the valued calculated by this function.
      - An alternative derivation is to apply Holder's inequality to `signal`,
        using the constant function `1` on the support of the `signal` as the
        2nd function.

    Interpretation:
      - If we apply this function to returns in the case where the expected
        value of returns is 0 and we take p_norm = 2, then the result of this
        function can be interpreted as a renormalized realized (inverse)
        volatility.
      - For a Gaussian signal, the expected value is np.sqrt(2 / np.pi), which
        is approximately 0.80. This holds regardless of the volatility of the
        Gaussian (so the measure is scale invariant).
      - For a stationary function, the expected value does not change with
        sampled series length.
      - For a signal that is t-distributed with 4 dof, the expected value is
        approximately 0.71.
    """
    hdbg.dassert_isinstance(signal, pd.Series)
    # Require that we evaluate a norm.
    hdbg.dassert_lte(1, p_norm)
    # TODO(*): Maybe add l-infinity support. For many stochastic signals, we
    # should not expect a finite value in the continuous limit.
    hdbg.dassert(np.isfinite(p_norm))
    # Set reasonable defaults for inf and nan modes.
    inf_mode = inf_mode or "drop"
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(signal, mode=nan_mode)
    nan_result = pd.Series(
        data=np.nan, index=[prefix + "jensen_ratio"], name=signal.name
    )
    hdbg.dassert(not data.isna().any())
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
    hdbg.dassert(data.apply(np.isfinite).all())
    # Return NaN if there is no data.
    if data.size == 0:
        _LOG.warning("Empty input signal `%s`", signal.name)
        return nan_result
    # Calculate norms.
    lp = sp.linalg.norm(data, ord=p_norm)
    l1 = sp.linalg.norm(data, ord=1)
    # Ignore support where `signal` has NaNs.
    scaled_support = data.size ** (1 - 1 / p_norm)
    jensen_ratio = l1 / (scaled_support * lp)
    res = pd.Series(
        data=[jensen_ratio], index=[prefix + "jensen_ratio"], name=signal.name
    )
    return res


def compute_t_distribution_j_2(nu: float):
    """
    Compute the Jensen ratio with `p_norm = 2` for a standard t-distribution.

    :param nu: degrees of freedom
    :return: the theoretical J-ratio for a standard t-distribution with `nu`
        degrees of freedom.
    """
    hdbg.dassert_lte(2, nu, "The Jensen ratio is only well defined for nu >= 2.")
    if nu == np.inf:
        jensen_2 = np.sqrt(2 / np.pi)
    elif nu == 2:
        jensen_2 = 0
    else:
        if nu > 300:
            _LOG.warning("Computation is unstable for large values of `nu`.")
        const = 2 / np.sqrt(np.pi)
        dist = sp.special.gamma((nu + 1) / 2) / (
            sp.special.gamma(nu / 2) * (nu - 1)
        )
        jensen_2 = const * dist * np.sqrt(nu - 2)
    return jensen_2


def _check_alpha_and_normalize_data(data: pd.Series, alpha: float) -> pd.Series:
    """
    Check assumptions used in surprise, diversity, and entropy functions.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    """
    hdbg.dassert_lte(
        0, alpha, "Parameter `alpha` must be greater than or equal to 0."
    )
    hdbg.dassert_isinstance(data, pd.Series)
    hdbg.dassert(
        (data >= 0).all(), "Series `data` must have only nonnegative values."
    )
    # Normalize nonnegative data so that it sums to one.
    normalized_data = data / data.sum()
    return normalized_data


def compute_surprise(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-surprise per entry after probability normalizing `data`.

    This treats `data` as a probability space. All values of `data` must be
    nonnegative. Before calculating surprise, the data is renormalized so that
    its sum is one.

    See the following for details:
    https://golem.ph.utexas.edu/category/2008/11/entropy_diversity_and_cardinal_1.html

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    """
    hdbg.dassert_ne(
        1, alpha, "The special case `alpha=1` must be handled separately."
    )
    normalized_data = _check_alpha_and_normalize_data(data, alpha)
    surprise = (1 - normalized_data ** (alpha - 1)) / (alpha - 1)
    return surprise


def compute_diversity(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-diversity of `data` after probability normalizing.

    Special cases:
      - alpha = 0: `data.count() - 1`
      - alpha = 1: Shannon entropy
      - alpha = 2: Simpson diversity
      - alpha = np.inf: 0

    Conceptually, this can be calculated by
        ```
        surpise = compute_surprise(data, alpha)
        normalized_data = data / data.sum()
        diversity = (normalized_data * surprise).sum()
        ```
    We implement the function differently so as to avoid numerical instability.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    :return: a number between 0 and the surprise of `1 / data.count()`.
    """
    normalized_data = _check_alpha_and_normalize_data(data, alpha)
    if alpha == 1:
        log_normalized_data = np.log(normalized_data)
        entropy = -(normalized_data * log_normalized_data).sum()
        diversity = np.exp(entropy)
    else:
        sum_of_powers = (normalized_data ** alpha).sum()
        diversity = (1 - sum_of_powers) / (alpha - 1)
    return diversity

def _check_alpha_and_normalize_data(data: pd.Series, alpha: float) -> pd.Series:
    """
    Check assumptions used in surprise, diversity, and entropy functions.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    """
    hdbg.dassert_lte(
        0, alpha, "Parameter `alpha` must be greater than or equal to 0."
    )
    hdbg.dassert_isinstance(data, pd.Series)
    hdbg.dassert(
        (data >= 0).all(), "Series `data` must have only nonnegative values."
    )
    # Normalize nonnegative data so that it sums to one.
    normalized_data = data / data.sum()
    return normalized_data


def compute_surprise(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-surprise per entry after probability normalizing `data`.

    This treats `data` as a probability space. All values of `data` must be
    nonnegative. Before calculating surprise, the data is renormalized so that
    its sum is one.

    See the following for details:
    https://golem.ph.utexas.edu/category/2008/11/entropy_diversity_and_cardinal_1.html

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    """
    hdbg.dassert_ne(
        1, alpha, "The special case `alpha=1` must be handled separately."
    )
    normalized_data = _check_alpha_and_normalize_data(data, alpha)
    surprise = (1 - normalized_data ** (alpha - 1)) / (alpha - 1)
    return surprise


def compute_diversity(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-diversity of `data` after probability normalizing.

    Special cases:
      - alpha = 0: `data.count() - 1`
      - alpha = 1: Shannon entropy
      - alpha = 2: Simpson diversity
      - alpha = np.inf: 0

    Conceptually, this can be calculated by
        ```
        surpise = compute_surprise(data, alpha)
        normalized_data = data / data.sum()
        diversity = (normalized_data * surprise).sum()
        ```
    We implement the function differently so as to avoid numerical instability.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    :return: a number between 0 and the surprise of `1 / data.count()`.
    """
    normalized_data = _check_alpha_and_normalize_data(data, alpha)
    if alpha == 1:
        log_normalized_data = np.log(normalized_data)
        entropy = -(normalized_data * log_normalized_data).sum()
        diversity = np.exp(entropy)
    else:
        sum_of_powers = (normalized_data ** alpha).sum()
        diversity = (1 - sum_of_powers) / (alpha - 1)
    return diversity


def compute_cardinality(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-cardinality of `data` after probability normalizing.

    Special cases:
      - alpha = 0: `data.count()`
      - alpha = 1: exp(Shannon entropy)
      - alpha = 2: reciprocal Simpson
      - alpha = np.inf: 1 / max(normalized data)

    This is the exponential of the alpha-entropy.

    Conceptually, this can be calculated by
        ```
        diversity = compute_diversity(data, alpha)
        inverse_surprise = (1 - (alpha - 1) * diversity) ** (1 / (alpha - 1))
        cardinality = 1 / inverse_surprise
        ```
    We implement the function differently so as to avoid numerical instability.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    :return: a number between 1 and `data.count()`
    """
    normalized_data = _check_alpha_and_normalize_data(data, alpha)
    if np.isinf(alpha):
        cardinality = 1 / normalized_data.max()
    elif alpha == 1:
        log_normalized_data = np.log(normalized_data)
        entropy = -(normalized_data * log_normalized_data).sum()
        cardinality = np.exp(entropy)
    else:
        sum_of_powers = (normalized_data ** alpha).sum()
        cardinality = sum_of_powers ** (1 / (1 - alpha))
    return cardinality


def compute_entropy(data: pd.Series, alpha: float) -> float:
    """
    Compute the alpha-entropy of `data` after probability normalizing.

    Special cases:
      - alpha = 0: `log(data.count())`
      - alpha = 1: Shannon entropy
      - alpha = 2: log reciprocal Simpson
      - alpha = np.inf: log reciprocal Berger-Parker

    This is the log of of the alpha-cardinality.

    :param data: series of nonnegative numbers
    :param alpha: parameter in [0, np.inf]
    :return: a number between 0 and `log(data.count())`
    """
    cardinality = compute_cardinality(data, alpha)
    entropy = np.log(cardinality)
    return entropy


# TODO(Paul): Deprecate and replace with `compute_cardinality()`.
def compute_hill_number(data: pd.Series, q: float) -> float:
    """
    Compute the Hill number as a measure of diversity.

    - The output is a number between zero and the number of points in `data`
    - The result can be used as an "effective count"
    - Note that q = 1 corresponds to exp(Shannon entropy)
    - The q = np.inf case is used in the calculation of stable rank

    Transformations of the Hill number can be related to
    - Shannon index
    - Renyi entropy
    - Simpson index
    - Gini-Simpson index
    - Berger-Parker index
    See https://en.wikipedia.org/wiki/Diversity_index for details.

    :param data: data representing class counts or probabilities
    :param q: order of the Hill number
      - q = 1 provides an entropy measure
      - increasing q puts more weight on top relative classes
    :return: a float between zero and data.size
    """
    hdbg.dassert_lte(1, q, "Order `q` must be greater than or equal to 1.")
    hdbg.dassert_isinstance(data, pd.Series)
    hdbg.dassert(
        (data >= 0).all(), "Series `data` must have only nonnegative values."
    )
    hill_number = compute_cardinality(data, q)
    return hill_number


def get_symmetric_normal_quantiles(bin_width: float) -> list:
    """
    Get centered quantiles of normal distribution.

    This function creates a centered bin of width `bin_width` about zero and
    then adds bins on either side until the entire distribution is captured.
    All bins carry an equal percentage of the normal distribution, with the
    possible exception of the two tail bins.

    :param bin_width: percentage of normal distribution to capture in each bin
    :return: ordered endpoints of bins, including `-np.inf` and `np.inf`
    """
    hdbg.dassert_lt(0, bin_width)
    hdbg.dassert_lt(bin_width, 1)
    half_bin_width = bin_width / 2
    positive_bin_boundaries = [
        sp.stats.norm.ppf(x + 0.5)
        for x in np.arange(half_bin_width, 0.5, bin_width)
    ]
    positive_bin_boundaries.append(np.inf)
    negative_bin_boundaries = [-x for x in reversed(positive_bin_boundaries)]
    bin_boundaries = negative_bin_boundaries + positive_bin_boundaries
    return bin_boundaries


def group_by_bin(
    df: pd.DataFrame,
    bin_col: Union[str, int],
    bin_width: float,
    aggregation_col: Union[str, int],
) -> pd.DataFrame:
    """
    Compute aggregations in `aggregation_col` according to bins from `bin_col`.

    :param df: dataframe with numerical cols
    :param bin_col: a column used for binning; ideally the values are centered
        and approximately normally distributed, though not necessarily
        standardized
    :param bin_width: the percentage of data to be captured by each (non-tail)
        bin
    :param aggregation_col: the numerical col to aggregate
    :return: dataframe with count, mean, stdev of `aggregation_col` by bin
    """
    # Get bin boundaries assuming a normal distribution. Using theoretical
    # boundaries rather than empirical ones facilities comparisons across
    # different data.
    bin_boundaries = get_symmetric_normal_quantiles(bin_width)
    # Standardize the binning column and cut.
    normalized_bin_col = df[bin_col] / df[bin_col].std()
    cuts = pd.cut(normalized_bin_col, bin_boundaries)
    # Group the aggregation column according to the bins.
    grouped_col_values = df.groupby(cuts)[aggregation_col]
    # Aggregate the grouped result.
    count = grouped_col_values.count().rename("count")
    mean = grouped_col_values.mean().rename("mean")
    stdev = grouped_col_values.std().rename("stdev")
    # Join aggregation and counts.
    result_df = pd.concat(
        [
            count,
            mean,
            stdev,
        ],
        axis=1,
    )
    return result_df


# #############################################################################
# Stationarity statistics
# #############################################################################


# TODO(*): Maybe add `inf_mode`.
def apply_adf_test(
    srs: pd.Series,
    maxlag: Optional[int] = None,
    regression: Optional[str] = None,
    autolag: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Implement a wrapper around statsmodels' adfuller test.

    :param srs: pandas series of floats
    :param maxlag: as in stattools.adfuller
    :param regression: as in stattools.adfuller
    :param autolag: as in stattools.adfuller
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue, and related info
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    regression = regression or "c"
    autolag = autolag or "AIC"
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    # Hack until we factor out inf handling.
    srs = srs.replace([np.inf, -np.inf], np.nan)
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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
    """
    Implement a wrapper around statsmodels' KPSS test.

    http://debis.deu.edu.tr/userweb//onder.hanedar/dosyalar/kpss.pdf

    :param srs: pandas series of floats
    :param regression: as in stattools.kpss
    :param nlags: as in stattools.kpss
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue, and related info
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    regression = regression or "c"
    nan_mode = nan_mode or "drop"
    nlags = nlags or "auto"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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


# #############################################################################
# Normality statistics
# #############################################################################


def apply_normality_test(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Test (indep) null hypotheses that each col is normally distributed.

    An omnibus test of normality that combines skew and kurtosis.

    :param prefix: optional prefix for metrics' outcome
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :return: series with statistics and p-value
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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


def compute_centered_gaussian_total_log_likelihood(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Compute the log likelihood that `srs` came from a centered Gaussian.
    """
    prefix = prefix or ""
    # Calculate variance assuming a population mean of zero.
    var = np.square(srs).mean()
    name = str(srs.name) + "var"
    var_srs = pd.Series(index=srs.index, data=var, name=name)
    df = pd.concat([srs, var_srs], axis=1)
    df_out = csigproc.compute_centered_gaussian_log_likelihood(
        df, observation_col=srs.name, variance_col=name
    )
    log_likelihood = df_out["log_likelihood"].sum()
    result = pd.Series(
        data=[log_likelihood, var],
        index=[prefix + "log_likelihood", prefix + "centered_var"],
        name=srs.name,
    )
    return result


# #############################################################################
# Autocorrelation and cross-correlation statistics
# #############################################################################


def apply_ljung_box_test(
    srs: pd.Series,
    lags: Optional[Union[int, pd.Series]] = None,
    model_df: Optional[int] = None,
    period: Optional[int] = None,
    return_df: Optional[bool] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """
    Implement a wrapper around statsmodels' Ljung-Box test.

    :param srs: pandas series of floats
    :param lags: as in diagnostic.acorr_ljungbox
    :param model_df: as in diagnostic.acorr_ljungbox
    :param period: as in diagnostic.acorr_ljungbox
    :param return_df: as in diagnostic.acorr_ljungbox
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: test statistic, pvalue
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    model_df = model_df or 0
    return_df = return_df or True
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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


def compute_cross_correlation(
    df: pd.DataFrame,
    x_cols: List[Union[int, str]],
    y_col: Union[int, str],
    lags: List[int],
) -> pd.DataFrame:
    """
    Compute cross-correlations between `x_cols` and `y_col` at `lags`.

    :param df: data dataframe
    :param x_cols: x variable columns
    :param y_col: y variable column
    :param lags: list of integer lags to shift `x_cols` by
    :return: dataframe of cross correlation at lags
    """
    hdbg.dassert(not df.empty, msg="Dataframe must be nonempty")
    hdbg.dassert_isinstance(x_cols, list)
    hdbg.dassert_is_subset(x_cols, df.columns)
    hdbg.dassert_isinstance(y_col, (int, str))
    hdbg.dassert_in(y_col, df.columns)
    hdbg.dassert_isinstance(lags, list)
    # Drop rows with no y value.
    _LOG.debug("y_col=`%s` count=%i", y_col, df[y_col].count())
    df = df.dropna(subset=[y_col])
    x_vars = df[x_cols]
    y_var = df[y_col]
    correlations = []
    for lag in lags:
        corr = x_vars.shift(lag).apply(lambda x: x.corr(y_var))
        corr.name = lag
        correlations.append(corr)
    corr_df = pd.concat(correlations, axis=1)
    return corr_df.transpose()


# #############################################################################
# Spectral statistics
# #############################################################################


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
    hdbg.dassert_isinstance(signal, pd.Series)
    nan_mode = nan_mode or "fill_with_zero"
    inf_mode = inf_mode or "drop"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(signal, mode=nan_mode)
    has_infs = (~data.apply(np.isfinite)).any()
    # Make an output for empty or too short inputs.
    nan_result = pd.Series(
        data=[np.nan],
        index=[prefix + "forecastability"],
        name=signal.name,
    )
    if has_infs:
        if inf_mode == "return_nan":
            return nan_result
        if inf_mode == "drop":
            # Replace inf with np.nan and drop.
            data = data.replace([-np.inf, np.inf], np.nan).dropna()
        else:
            raise ValueError(f"Unrecognized inf_mode `{inf_mode}")
    hdbg.dassert(data.apply(np.isfinite).all())
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


def compute_swt_var_summary(
    sig: Union[pd.DataFrame, pd.Series],
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute swt var using scales up to `depth`.

    Params as in `csigproc.get_swt()`.
    """
    swt_var = csigproc.compute_swt_var(
        sig, wavelet=wavelet, depth=depth, timing_mode=timing_mode, axis=0
    )
    hdbg.dassert_in("swt_var", swt_var.columns)
    srs = csigproc.compute_swt_var(
        sig, wavelet=wavelet, depth=depth, timing_mode=timing_mode, axis=1
    )
    fvi = srs.first_valid_index()
    decomp = swt_var / srs.loc[fvi:].count()
    decomp["cum_swt_var"] = decomp["swt_var"].cumsum()
    decomp["perc"] = decomp["swt_var"] / sig.loc[fvi:].var()
    decomp["cum_perc"] = decomp["perc"].cumsum()
    return decomp


def compute_swt_covar_summary(
    df: pd.DataFrame,
    col1: str,
    col2: str,
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute swt var using scales up to `depth`.

    Params as in `csigproc.get_swt()`.
    """
    hdbg.dassert_ne(col1, col2)
    swt_covar = csigproc.compute_swt_covar(
        df,
        col1,
        col2,
        wavelet=wavelet,
        depth=depth,
        timing_mode=timing_mode,
        axis=0,
    )
    hdbg.dassert_in("swt_covar", swt_covar.columns)
    srs = csigproc.compute_swt_covar(
        df,
        col1,
        col2,
        wavelet=wavelet,
        depth=depth,
        timing_mode=timing_mode,
        axis=1,
    )
    fvi = srs.first_valid_index()
    decomp = swt_covar / srs.loc[fvi:].count()
    var1_col = str(col1) + "_swt_var"
    var2_col = str(col2) + "_swt_var"
    hdbg.dassert_in(var1_col, decomp.columns)
    hdbg.dassert_in(var2_col, decomp.columns)
    decomp["swt_corr"] = decomp["swt_covar"].divide(
        np.sqrt(decomp[var1_col].multiply(decomp[var2_col]))
    )
    return decomp


# #############################################################################
# Sharpe ratio
# #############################################################################


def summarize_sharpe_ratio(
    pnl: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate SR, SE(SR) from rets with an index freq and annualize.

    TODO(*): Consider de-biasing when the number of sample points is small,
        e.g., https://www.twosigma.com/wp-content/uploads/sharpe-tr-1.pdf
    """
    prefix = prefix or ""
    sr = compute_annualized_sharpe_ratio(pnl)
    sr_se_estimate = compute_annualized_sharpe_ratio_standard_error(pnl)
    res = pd.Series(
        data=[sr, sr_se_estimate],
        index=[prefix + "sharpe_ratio", prefix + "sharpe_ratio_standard_error"],
        name=pnl.name,
    )
    return res


def zscore_oos_sharpe_ratio(
    pnl: pd.Series, oos: Any, prefix: Optional[str] = None
) -> pd.Series:
    """
    Z-score the observed OOS SR wrt the INS SR and inflated SE.

    Calculate the following stats:
      - SR, SE(SR) for INS
      - SR, SE(SR) for OOS
      - z-scored OOS SR

    TODO(*): Consider factoring out pieces and/or returning more info.

    :param pnl: time series of per-period PnL
    :param oos: start of OOS (right endpoint)
    :param prefix: prefix for the output series index
    :return: series of SR stats
    """
    prefix = prefix or ""
    # Create ins/oos masks.
    ins_mask = pnl.index < oos
    hdbg.dassert(ins_mask.any())
    ins_nobs = ins_mask.sum()
    oos_mask = pnl.index >= oos
    hdbg.dassert(oos_mask.any())
    oos_nobs = oos_mask.sum()
    #
    inflation = compute_sharpe_ratio_prediction_interval_inflation_factor(
        ins_nobs, oos_nobs
    )
    #
    ins_srs = pnl.loc[ins_mask].copy()
    oos_srs = pnl.loc[oos_mask].copy()
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
    pnl: Union[pd.Series, pd.DataFrame],
) -> Union[float, pd.Series]:
    """
    Compute SR from rets with an index freq and annualize.

    :param pnl: time series of per-period PnL
    :return: annualized Sharpe ratio
    """
    pnl = cofinanc.maybe_resample(pnl)
    points_per_year = hdatafr.infer_sampling_points_per_year(pnl)
    if isinstance(pnl, pd.Series):
        pnl = hdatafr.apply_nan_mode(pnl, mode="fill_with_zero")
    if isinstance(pnl, pd.DataFrame):
        pnl = pnl.apply(hdatafr.apply_nan_mode, mode="fill_with_zero")
    sr = compute_sharpe_ratio(pnl, points_per_year)
    return sr


def compute_annualized_sharpe_ratio_standard_error(
    pnl: Union[pd.Series, pd.DataFrame],
) -> Union[float, pd.Series]:
    """
    Compute SE(SR) from rets with an index freq and annualize.

    This function calculates the standard error with respect to the original
    sampling frequency and then rescales to turn it into a standard error
    for the corresponding annualized Sharpe ratio.

    :param pnl: time series of per-period PnL
    :return: standard error estimate of annualized Sharpe ratio
    """
    pnl = cofinanc.maybe_resample(pnl)
    points_per_year = hdatafr.infer_sampling_points_per_year(pnl)
    pnl = hdatafr.apply_nan_mode(pnl, mode="fill_with_zero")
    se_sr = compute_sharpe_ratio_standard_error(pnl, points_per_year)
    return se_sr


def compute_sharpe_ratio(
    pnl: Union[pd.Series, pd.DataFrame], time_scaling: Union[int, float] = 1
) -> Union[float, pd.Series]:
    r"""Calculate Sharpe Ratio (SR) from pnl and rescale.

    For a detailed exploration of SR, see
    http://www.gilgamath.com/pages/ssc.html.

    :param pnl: time series of per-period PnL
    :param time_scaling: rescales SR by a factor of \sqrt(time_scaling).
        - For SR with respect to the sampling frequency, set equal to 1
        - For annualization, set equal to the number of sampling frequency
          ticks per year (e.g., =252 if daily returns are provided)
    :return: Sharpe Ratio
    """
    hdbg.dassert_lte(1, time_scaling, f"time_scaling=`{time_scaling}`")
    sr = pnl.mean() / pnl.std()
    sr *= np.sqrt(time_scaling)
    if isinstance(sr, pd.Series):
        sr.name = "SR"
    return sr


def compute_sharpe_ratio_standard_error(
    pnl: Union[pd.Series, pd.DataFrame], time_scaling: Union[int, float] = 1
) -> Union[float, pd.Series]:
    """
    Calculate Sharpe Ratio standard error from pnl and rescale.

    :param pnl: time series of per-period PnL
    :param time_scaling: as in `compute_sharpe_ratio`
    :return: Sharpe ratio standard error estimate
    """
    hdbg.dassert_lte(1, time_scaling, f"time_scaling=`{time_scaling}`")
    # Compute the Sharpe ratio using the sampling frequency units[
    sr = compute_sharpe_ratio(pnl, time_scaling=1)
    srs_size = hdatafr.apply_nan_mode(pnl, mode="drop").size
    hdbg.dassert_lt(1, srs_size)
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
        hdbg.dassert_isinstance(sharpe_ratio, numbers.Number)
        years = const / (sharpe_ratio ** 2)
    elif years is not None and sharpe_ratio is None:
        hdbg.dassert_isinstance(years, numbers.Number)
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
    """
    Compute the constant to use in the t-test power law.

    E.g., http://www.vanbelle.org/chapters/webchapter2.pdf

    A special case of this is known as Lehr's rule.

    :param alpha: type I error rate
    :param power: 1 - type II error rate
    :param two_sided: one or two-sided t-test
    :return: constant to use in (one sample) t-test power law
    """
    hdbg.dassert_lt(0, alpha)
    hdbg.dassert_lt(alpha, 1)
    hdbg.dassert_lt(0, power)
    hdbg.dassert_lt(power, 1)
    if two_sided:
        alpha /= 2
    const: float = (sp.stats.norm.ppf(1 - alpha) + sp.stats.norm.ppf(power)) ** 2
    return const


def compute_sharpe_ratio_prediction_interval_inflation_factor(
    ins_nobs: Union[int, float], oos_nobs: Union[int, float]
) -> float:
    """
    Compute the SE(SR) inflation factor for obtaining conditional OOS bounds.

    :param ins_nobs: number of observations in-sample
    :param oos_nobs: number of observations out-of-sample
    :return: float > 1
    """
    se_inflation_factor = np.sqrt(1 + ins_nobs / oos_nobs)
    se_inflation_factor = cast(float, se_inflation_factor)
    return se_inflation_factor


def apply_sharpe_ratio_correlation_conversion(
    points_per_year: float,
    sharpe_ratio: Optional[float] = None,
    correlation: Optional[float] = None,
) -> float:
    """
    Convert annualized SR to correlation or vice-versa.

    :param points_per_year: number of trading periods per year
    :param sharpe_ratio: annualized Sharpe ratio
    :param correlation: correlation coefficient
    :return: annualized Sharpe ratio if correlation is provided; correlation
        if annualized Sharpe ratio is provided.
    """
    if sharpe_ratio is not None and correlation is None:
        sharpe_ratio /= np.sqrt(points_per_year)
        return sharpe_ratio / np.sqrt(1 - sharpe_ratio ** 2)
    if sharpe_ratio is None and correlation is not None:
        sharpe_ratio = correlation / np.sqrt(1 + correlation ** 2)
        return sharpe_ratio * np.sqrt(points_per_year)
    raise ValueError(
        "Precisely one of `sharpe_ratio` and `correlation` should not be `None`"
    )


def compute_implied_correlation(pnl: pd.Series) -> float:
    """
    Infer correlation of prediction with returns given PnL.

    The inferred correlation is an estimate based on the following assumptions:
      1. Returns are Gaussian
      2. Positions are proportional to predictions
      3. Every value of `pnl` corresponds to a unique position and prediction
         (so the pnl frequency is the same as the prediction frequency)
    The principle is to estimate the correlation from the Sharpe ratio of
    `pnl`. This function addresses the issue of calculating the appropriate
    scaling factors required to perform the conversion correctly.

    :param pnl: PnL stream with `freq`
    :return: estimated correlation of the predictions that, when combined with
        returns, generated `pnl`
    """
    count_per_year = hdatafr.compute_count_per_year(pnl)
    sr = compute_annualized_sharpe_ratio(pnl)
    corr = apply_sharpe_ratio_correlation_conversion(
        count_per_year, sharpe_ratio=sr
    )
    return corr


def compute_implied_sharpe_ratio(srs: pd.Series, corr: float) -> float:
    """
    Infer implied Sharpe ratio given `corr` and predictions at `srs` non-NaNs.

    Same assumptions as `compute_implied_correlation()`, and `srs` must
    have a `DatetimeIndex` with a `freq`. The values of `srs` are not
    used directly, but rather only the knowledge of whether they are
    included in `srs.count()`, e.g., are non-NaN, non-inf, etc.
    """
    hdbg.dassert_lte(-1, corr)
    hdbg.dassert_lte(corr, 1)
    count_per_year = hdatafr.compute_count_per_year(srs)
    sr = apply_sharpe_ratio_correlation_conversion(
        count_per_year, correlation=corr
    )
    return sr


def compute_hit_rate_implied_by_correlation(
    corr: float, j_ratio: Optional[float] = None
) -> float:
    """
    Infer hit rate given `corr`.

    This approximation is only valid under certain distributional
    assumptions.
    """
    j_ratio = j_ratio or np.sqrt(2 / np.pi)
    hdbg.dassert_lt(0, j_ratio)
    hdbg.dassert_lte(j_ratio, 1)
    hdbg.dassert_lte(-1, corr)
    hdbg.dassert_lte(corr, 1)
    return sp.stats.norm.sf(-1 * corr / j_ratio)


def compute_correlation_implied_by_hit_rate(
    hit_rate: float, j_ratio: Optional[float] = None
) -> float:
    """
    Infer correlation given `hit_rate`. Assumes normal-like series.

    This inverts `compute_hit_rate_implied_by_correlation()` and is
    similarly only valid under certain distributional assumptions.
    """
    j_ratio = j_ratio or np.sqrt(2 / np.pi)
    hdbg.dassert_lt(0, j_ratio)
    hdbg.dassert_lte(j_ratio, 1)
    hdbg.dassert_lte(0, hit_rate)
    hdbg.dassert_lte(hit_rate, 1)
    return j_ratio * sp.stats.norm.ppf(hit_rate)


# #############################################################################
# Drawdown statistics
# #############################################################################


def compute_drawdown_cdf(
    sharpe_ratio: float,
    volatility: float,
    drawdown: float,
    time: Optional[float] = None,
) -> float:
    """
    Compute the drawdown cdf for `drawdown` at `time` given SR, vol specs.

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
    hdbg.dassert_lt(0, sharpe_ratio)
    hdbg.dassert_lt(0, volatility)
    hdbg.dassert_lt(0, drawdown)
    hdbg.dassert_lt(0, time)
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
    """
    Compute the drawdown cdf for drawdown given in units of volatility.

    :param sharpe_ratio: Sharpe ratio
    :param normalized_drawdown: drawdown in units of volatility, e.g.,
        (actual) drawdown / volatility
    :param time: time in units consistent with those of SR
    :return: Prob(normalized drawdown at time `time` <= `normalized_drawdown`)
    """
    hdbg.dassert_lt(0, sharpe_ratio)
    hdbg.dassert_lt(0, normalized_drawdown)
    hdbg.dassert_lt(0, time)
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
    """
    Compute the approximate cdf for the maximum drawdown over a span of time.

    - https://www.sciencedirect.com/science/article/pii/S0304414913001695
    - G. F. Newell, Asymptotic Extreme Value Distribution for One-dimensional
      Diffusion Processes

    TODO(*): Revisit units and rescaling.
    TODO(*): Maybe normalize drawdown.

    :return: estimate of
        Prob(max drawdown over time period of length `time` <= `max_drawdown`)
    """
    hdbg.dassert_lt(0, sharpe_ratio)
    hdbg.dassert_lt(0, volatility)
    hdbg.dassert_lt(0, max_drawdown)
    hdbg.dassert_lt(0, time)
    lambda_ = 2 * sharpe_ratio / volatility
    # lambda_ * max_drawdown is the same as
    #     -2 * sharpe_ratio * (max_drawdown / volatility)
    y = lambda_ * max_drawdown - np.log(time)
    probability: float = sp.stats.gumbel_r.cdf(y)
    return probability


def compute_max_drawdown(
    pnl: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate max drawdown statistic.

    :param pnl: series of per-period PnL
    :param prefix: optional prefix for metrics' outcome
    :return: max drawdown in units of PnL
    """
    hdbg.dassert_isinstance(pnl, pd.Series)
    prefix = prefix or ""
    result_index = [prefix + "max_drawdown"]
    nan_result = pd.Series(index=result_index, name=pnl.name, dtype="float64")
    if pnl.empty:
        _LOG.warning("Empty input series `%s`", pnl.name)
        return nan_result
    drawdown = cofinanc.compute_drawdown(pnl)
    max_drawdown = drawdown.max()
    result = pd.Series(data=max_drawdown, index=result_index, name=pnl.name)
    return result


# #############################################################################
# Returns
# #############################################################################


def compute_annualized_return_and_volatility(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Annualized mean return and sample volatility in %.

    :param srs: series with datetimeindex with `freq`
    :param prefix: optional prefix for metrics' outcome
    :return: annualized pd.Series with return and volatility in %; pct rets
        if `srs` consists of pct rets, log rets if `srs` consists of log rets.
    """
    hdbg.dassert_isinstance(srs, pd.Series)
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
    annualized_mean_return = 100 * cofinanc.compute_annualized_return(srs)
    annualized_volatility = 100 * cofinanc.compute_annualized_volatility(srs)
    result = pd.Series(
        data=[annualized_mean_return, annualized_volatility],
        index=result_index,
        name=srs.name,
    )
    return result


# #############################################################################
# Bet statistics
# #############################################################################


def compute_bet_stats(
    positions: pd.Series,
    log_rets: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate average returns for grouped bets.

    :param positions: series of long/short positions
    :param log_rets: log returns
    :param prefix: optional prefix for metrics' outcome
    :return: series of average returns for winning/losing and long/short bets,
        number of positions and bets. In `average_num_bets_per_year`, "year" is
        not the calendar year, but an approximate number of data points in a
        year
    """
    prefix = prefix or ""
    bet_lengths = cofinanc.compute_signed_bet_lengths(positions)
    log_rets_per_bet = cofinanc.compute_returns_per_bet(positions, log_rets)
    #
    stats = dict()
    stats["num_positions"] = int(bet_lengths.abs().sum())
    stats["num_bets"] = bet_lengths.size
    stats["long_bets_(%)"] = 100 * (bet_lengths > 0).sum() / bet_lengths.size
    if positions.index.freq is not None:
        n_years = positions.size / hdatafr.infer_sampling_points_per_year(
            positions
        )
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
    stats[
        "avg_return_winning_bets_(%)"
    ] = 100 * cofinanc.convert_log_rets_to_pct_rets(avg_ret_winning_bets)
    avg_ret_losing_bets = log_rets_per_bet.loc[log_rets_per_bet < 0].mean()
    stats[
        "avg_return_losing_bets_(%)"
    ] = 100 * cofinanc.convert_log_rets_to_pct_rets(avg_ret_losing_bets)
    avg_ret_long_bet = log_rets_per_bet.loc[bet_lengths > 0].mean()
    stats[
        "avg_return_long_bet_(%)"
    ] = 100 * cofinanc.convert_log_rets_to_pct_rets(avg_ret_long_bet)
    avg_ret_short_bet = log_rets_per_bet.loc[bet_lengths < 0].mean()
    stats[
        "avg_return_short_bet_(%)"
    ] = 100 * cofinanc.convert_log_rets_to_pct_rets(avg_ret_short_bet)
    #
    srs = pd.Series(stats, name=log_rets.name)
    srs.index = prefix + srs.index
    return srs


# #############################################################################
# Hit rate: MDA
# #############################################################################


def calculate_hit_rate(
    srs: pd.Series,
    alpha: Optional[float] = None,
    method: Optional[str] = None,
    threshold: Optional[float] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Calculate hit rate statistics.

    :param srs: pandas series
    :param alpha: as in statsmodels.stats.proportion.proportion_confint()
    :param method: as in statsmodels.stats.proportion.proportion_confint()
    :param threshold: threshold value around zero to exclude from calculations
    :param prefix: optional prefix for metrics' outcome
    :return: hit rate statistics: point estimate, lower bound, upper bound
    """
    alpha = alpha or 0.05
    method = method or "jeffreys"
    hdbg.dassert_lte(0, alpha)
    hdbg.dassert_lte(alpha, 1)
    hdbg.dassert_isinstance(srs, pd.Series)
    threshold = threshold or 0
    hdbg.dassert_lte(0, threshold)
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
    srs = hdatafr.apply_nan_mode(srs, mode="drop")
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
# Turnover statistics
# #############################################################################


def compute_avg_turnover_and_holding_period(
    pos: pd.Series,
    unit: Optional[str] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Compute average turnover and holding period for a sequence of positions.

    :param pos: pandas series of positions
    :param unit: desired output holding period unit (e.g. 'B', 'W', 'M', etc.)
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: average turnover, holding period and index frequency
    """
    hdbg.dassert_isinstance(pos, pd.Series)
    hdbg.dassert(pos.index.freq)
    pos_freq = pos.index.freq
    unit = unit or pos_freq
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    result_index = [
        prefix + "avg_turnover_(%)",
        prefix + "turnover_frequency",
        prefix + "avg_holding_period",
        prefix + "holding_period_units",
    ]
    avg_holding_period = cofinanc.compute_average_holding_period(
        pos=pos, unit=unit, nan_mode=nan_mode
    )
    avg_turnover = 100 * (1 / avg_holding_period)
    #
    result_values = [avg_turnover, unit, avg_holding_period, unit]
    res = pd.Series(data=result_values, index=result_index, name=pos.name)
    return res


# #############################################################################
# Hypothesis testing
# #############################################################################


def ttest_1samp(
    srs: pd.Series,
    popmean: Optional[float] = None,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Thin wrapper around scipy's ttest.

    :param srs: input series for computing statistics
    :param popmean: assumed population mean for test
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series with t-value and p-value
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    popmean = popmean or 0
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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
    """
    Wrap statsmodel's multipletests.

    Returns results in a series indexed like srs.
    Documentation at
    https://www.statsmodels.org/stable/generated/statsmodels.stats.multitest.multipletests.html

    :param srs: Series with pvalues
    :param method: `method` for scipy's multipletests
    :param nan_mode: approach to deal with NaNs, can be "strict" or "drop"
    :param prefix: optional prefix for metrics' outcome
    :return: Series of adjusted p-values
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    method = method or "fdr_bh"
    nan_mode = nan_mode or "strict"
    hdbg.dassert_in(nan_mode, ["strict", "drop"])
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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
    """
    Combine ttest and multitest pvalue adjustment.
    """
    popmean = popmean or 0
    nan_mode = nan_mode or "drop"
    method = method or "fdr_bh"
    prefix = prefix or ""
    hdbg.dassert_isinstance(data, pd.DataFrame)
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


def estimate_q_values(
    p_values: Union[pd.Series, pd.DataFrame],
    n_experiments: Optional[int] = None,
    pi0: Optional[float] = None,
) -> pd.Series:
    """
    Estimate q-values from p-values.

    See https://github.com/nfusi/qvalue for reference.

    :param p_values: p-values of test statistics of experiments
    :param n_experiments: number of experiments; by default, this is set to the
        number of p-values
    :param pi0: estimate of proportion of true null hypotheses
    :return: estimated q-values
    """
    # TODO(Paul): Relax the NaN constraint.
    hdbg.dassert(not p_values.isna().any().any())
    # Each p-value must lie in [0, 1].
    hdbg.dassert_lte(0, p_values.min().min())
    hdbg.dassert_lte(p_values.max().max(), 1)
    #
    pv = p_values.values.ravel()
    #
    n_experiments = n_experiments or len(pv)
    # Estimate the proportion of true null hypotheses.
    if pi0 is not None:
        # Use `pi0` if the caller provided one.
        pi0 = pi0
    elif n_experiments < 100:
        # Use the most conservative estimate if `n_experiments` is small.
        pi0 = 1.0
    else:
        # Follow the estimation procedure in Storey and Tibshirani (2003).
        pi0 = []
        lambdas = np.arange(0, 0.9, 0.01)
        counts = np.array([(pv > i).sum() for i in lambdas])
        for k in range(len(lambdas)):
            pi0.append(counts[k] / (n_experiments * (1 - lambdas[k])))
        #
        pi0 = np.array(pi0)
        # Fit natural cubic spline.
        tck = sp.interpolate.splrep(lambdas, pi0, k=3)
        pi0 = sp.interpolate.splev(lambdas[-1], tck)
        _LOG.info("pi0=%.3f (estimated proportion of null features" % pi0)
        if pi0 > 1:
            _LOG.info("pi0 > 1 (%.3f), clipping to 1" % pi0)
            pi0 = 1.0
    hdbg.dassert_lte(0, pi0)
    hdbg.dassert_lte(pi0, 1)
    #
    p_ordered = np.argsort(pv)
    pv = pv[p_ordered]
    # Initialize q-values.
    q_values = pi0 * n_experiments / len(pv) * pv
    q_values[-1] = min(q_values[-1], 1.0)
    # Iteratively compute q-values.
    for i in range(len(pv) - 2, -1, -1):
        q_values[i] = min(
            pi0 * n_experiments * pv[i] / (i + 1.0), q_values[i + 1]
        )
    # Reorder q-values.
    qv_temp = q_values.copy()
    qv = np.zeros_like(q_values)
    qv[p_ordered] = qv_temp
    #
    qv = qv.reshape(p_values.shape)
    #
    if isinstance(p_values, pd.Series):
        q_values = pd.Series(index=p_values.index, data=qv, name="q_val")
    else:
        q_values = pd.DataFrame(
            index=p_values.index, data=qv, columns=p_values.columns
        )
    return q_values


# #############################################################################
# Interarrival time statistics
# #############################################################################


def get_interarrival_time(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
) -> Optional[pd.Series]:
    """
    Get interrarival time from index of a time series.

    :param srs: pandas series of floats
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :return: series with interrarival time
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
    if data.empty:
        _LOG.warning("Empty input `%s`", srs.name)
        return None
    index = data.index
    # Check index of a series. We require that the input
    #     series have a sorted datetime index.
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    hpandas.dassert_strictly_increasing_index(index)
    # Compute a series of interrairival time.
    interrarival_time = pd.Series(index).diff()
    return interrarival_time


def compute_interarrival_time_stats(
    srs: pd.Series,
    nan_mode: Optional[str] = None,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Compute interarrival time statistics.

    :param srs: pandas series of interrarival time
    :param nan_mode: argument for hdatafr.apply_nan_mode()
    :param prefix: optional prefix for metrics' outcome
    :return: series with statistic and related info
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mode = nan_mode or "drop"
    prefix = prefix or ""
    data = hdatafr.apply_nan_mode(srs, mode=nan_mode)
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


# #############################################################################
# Models
# #############################################################################


def compute_regression_coefficients(
    df: pd.DataFrame,
    x_cols: List[Union[int, str]],
    y_col: Union[int, str],
    sample_weight_col: Optional[Union[int, str]] = None,
) -> pd.DataFrame:
    """
    Regresses `y_col` on each `x_col` independently.

    This function assumes (but does not check) that the x and y variables are
    centered.

    :param df: data dataframe
    :param x_cols: x variable columns
    :param y_col: y variable column
    :param sample_weight_col: optional nonnegative sample observation weights.
        If `None`, then equal weights are used. Weights do not need to be
        normalized.
    :return: dataframe of regression coefficients and related stats
    """
    hdbg.dassert(not df.empty, msg="Dataframe must be nonempty")
    hdbg.dassert_isinstance(x_cols, list)
    hdbg.dassert_is_subset(x_cols, df.columns)
    hdbg.dassert_isinstance(y_col, (int, str))
    hdbg.dassert_in(y_col, df.columns)
    # Sanity check weight column, if available. Set weights uniformly to 1 if
    # not specified.
    if sample_weight_col is not None:
        hdbg.dassert_in(sample_weight_col, df.columns)
        weights = df[sample_weight_col].rename("weight")
    else:
        weights = pd.Series(index=df.index, data=1, name="weight")
    # Ensure that no weights are negative.
    hdbg.dassert(not (weights < 0).any())
    # Ensure that the total weight is positive.
    hdbg.dassert((weights > 0).any())
    # Drop rows with no y value.
    _LOG.debug("y_col=`%s` count=%i", y_col, df[y_col].count())
    df = df.dropna(subset=[y_col])
    # Reindex weights to reflect any dropped y values.
    weights = weights.reindex(df.index)
    # Extract x variables.
    x_vars = df[x_cols]
    x_var_counts = x_vars.count().rename("count")
    # Create a per-`x_col` weight dataframe to reflect possibly different NaN
    # positions. This is used to generate accurate weighted sums and effective
    # sample sizes.
    weight_df = pd.DataFrame(index=x_vars.index, columns=x_cols)
    for col in x_cols:
        weight_df[col] = weights.reindex(x_vars[col].dropna().index)
    weight_sums = weight_df.sum(axis=0)
    # We use the 2-cardinality of the weights. This is equivalent to using
    # Kish's effective sample size.
    x_var_eff_counts = weight_df.apply(
        lambda x: compute_cardinality(x.dropna(), 2)
    ).rename("eff_count")
    # Calculate variance assuming x variables are centered at zero.
    x_variance = (
        x_vars.pow(2)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("var")
    )
    # Calculate covariance assuming x variables and y variable are centered.
    covariance = (
        x_vars.multiply(df[y_col], axis=0)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("covar")
    )
    # Calculate y variance assuming variable is centered.
    # NOTE: We calculate only one estimate of the variance of y, using all
    #     available data (regardless of whether a particular `x_col` is NaN or
    #     not). If samples of `x_cols` are not substantially aligned, then this
    #     may be undesirable.
    y_variance = df[y_col].pow(2).multiply(weights).sum() / weights.sum()
    _LOG.debug("y_col=`%s` variance=%f", y_col, y_variance)
    # Calculate correlation from covariances and variances.
    rho = covariance.divide(np.sqrt(x_variance) * np.sqrt(y_variance)).rename(
        "rho"
    )
    # Calculate beta coefficients and associated statistics.
    beta = covariance.divide(x_variance).rename("beta")
    # The `x_var_eff_counts` term makes this invariant with respect to
    # rescalings of the weight column.
    beta_se = np.sqrt(
        y_variance / (x_variance.multiply(x_var_eff_counts))
    ).rename("SE(beta)")
    z_scores = beta.divide(beta_se).rename("beta_z_scored")
    # Calculate two-sided p-values.
    p_val_array = 2 * sp.stats.norm.sf(z_scores.abs())
    p_val = pd.Series(index=z_scores.index, data=p_val_array, name="p_val_2s")
    # Calculate autocovariance-related stats of x variables.
    autocovariance = (
        x_vars.multiply(x_vars.shift(1), axis=0)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("autocovar")
    )
    # Normalize autocovariance to get autocorrelation.
    autocorrelation = autocovariance.divide(x_variance).rename("autocorr")
    turn = np.sqrt(2 * (1 - autocorrelation)).rename("turn")
    # Consolidate stats.
    coefficients = [
        x_var_counts,
        x_var_eff_counts,
        x_variance,
        covariance,
        rho,
        beta,
        beta_se,
        z_scores,
        p_val,
        autocovariance,
        autocorrelation,
        turn,
    ]
    return pd.concat(coefficients, axis=1)


def apply_smoothing_parameters(
    rho: pd.Series, turn: pd.Series, parameters: List[float]
) -> pd.DataFrame:
    """
    Estimate smoothing effects.

    :param parameters: corresponds to (inverse) exponent of `turn`
    """
    rhos = []
    turns = []
    tsq = turn ** 2
    for param in parameters:
        rho_num = np.square(np.linalg.norm(tsq.pow(-1 * param / 4).multiply(rho)))
        # TODO(Paul): Cross-check.
        turn_num = np.linalg.norm(tsq.pow(0.5 - 2 * param / 4).multiply(rho))
        denom = np.linalg.norm(tsq.pow(-2 * param / 4).multiply(rho))
        rhos.append(rho_num / denom)
        turns.append(turn_num / denom)
    rho_srs = pd.Series(index=parameters, data=rhos, name="rho")
    rho_frac = (rho_srs / rho_srs.max()).rename("rho_frac")
    turn_srs = pd.Series(index=parameters, data=turns, name="turn")
    rho_to_turn = (rho_srs / turn_srs).rename("rho_to_turn")
    df = pd.concat([rho_srs, rho_frac, turn_srs, rho_to_turn], axis=1)
    return df


def compute_local_level_model_stats(
    srs: pd.Series,
    prefix: Optional[str] = None,
) -> pd.Series:
    """
    Compute statistics assuming a steady-state local level model.

    E.g.,
       y_t = \mu_t + \epsilon_t
       \mu_{t + 1} = \mu_t + \eta_t

    This is equivalent to modeling `srs` of {y_t} as a random walk + noise at
    steady-state, or as ARIMA(0,1,1).

    See Harvey, Section 4.1.2, page 175.
    """
    prefix = prefix or ""
    hdbg.dassert_isinstance(srs, pd.Series)
    diff = srs.diff()
    demeaned_diff = diff - diff.mean()
    gamma0 = demeaned_diff.multiply(demeaned_diff).mean()
    gamma1 = demeaned_diff.multiply(demeaned_diff.shift(1)).mean()
    # This is the variance of \epsilon.
    var_epsilon = -1 * gamma1
    hdbg.dassert_lte(0, var_epsilon)
    # This is the variance of \eta.
    var_eta = gamma0 - 2 * var_epsilon
    hdbg.dassert_lte(0, var_eta)
    # This is the first autocorrelation coefficient.
    rho1 = gamma1 / gamma0
    # The signal-to-noise ratio `snr` is commonly denoted `q`.
    snr = var_eta / var_epsilon
    p = 0.5 * (snr + np.sqrt(snr ** 2 + 4 * snr))
    # Equivalent to EMA lambda in m_t = (1 - \lambda) m_{t - 1} + \lambda y_t.
    kalman_gain = p / (p + 1)
    # Convert to center-of-mass.
    com = 1 / kalman_gain - 1
    result_index = [
        prefix + "gamma0",
        prefix + "gamma1",
        prefix + "var_epsilon",
        prefix + "var_eta",
        prefix + "rho1",
        prefix + "snr",
        prefix + "kalman_gain",
        prefix + "com",
    ]
    result_values = [
        gamma0,
        gamma1,
        var_epsilon,
        var_eta,
        rho1,
        snr,
        kalman_gain,
        com,
    ]
    result = pd.Series(data=result_values, index=result_index, name=srs.name)
    return result


# #############################################################################
# Cross-validation
# #############################################################################


def get_rolling_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Partition index into chunks and returns pairs of successive chunks.

    If the index looks like
        [0, 1, 2, 3, 4, 5, 6]
    and n_splits = 4, then the splits would be
        [([0, 1], [2, 3]),
         ([2, 3], [4, 5]),
         ([4, 5], [6])]

    A typical use case is where the index is a monotonic increasing datetime
    index. For such cases, causality is respected by the splits.
    """
    hpandas.dassert_strictly_increasing_index(idx)
    n_chunks = n_splits + 1
    hdbg.dassert_lte(1, n_splits)
    # Split into equal chunks.
    chunk_size = int(math.ceil(idx.size / n_chunks))
    hdbg.dassert_lte(1, chunk_size)
    chunks = [idx[i : i + chunk_size] for i in range(0, idx.size, chunk_size)]
    hdbg.dassert_eq(len(chunks), n_chunks)
    #
    splits = list(zip(chunks[:-1], chunks[1:]))
    return splits


def get_oos_start_split(
    idx: pd.Index, datetime_: Union[datetime.datetime, pd.Timestamp]
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Split index using OOS (out-of-sample) start datetime.
    """
    hpandas.dassert_strictly_increasing_index(idx)
    ins_mask = idx < datetime_
    hdbg.dassert_lte(1, ins_mask.sum())
    oos_mask = ~ins_mask
    hdbg.dassert_lte(1, oos_mask.sum())
    ins = idx[ins_mask]
    oos = idx[oos_mask]
    return [(ins, oos)]


# TODO(Paul): Support train/test/validation or more.
def get_train_test_pct_split(
    idx: pd.Index, train_pct: float
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Split index into train and test sets by percentage.
    """
    hpandas.dassert_strictly_increasing_index(idx)
    hdbg.dassert_lt(0.0, train_pct)
    hdbg.dassert_lt(train_pct, 1.0)
    #
    train_size = int(train_pct * idx.size)
    hdbg.dassert_lte(0, train_size)
    train_split = idx[:train_size]
    test_split = idx[train_size:]
    return [(train_split, test_split)]


def get_expanding_window_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Generate splits with expanding overlapping windows.
    """
    hpandas.dassert_strictly_increasing_index(idx)
    hdbg.dassert_lte(1, n_splits)
    tscv = sklearn.model_selection.TimeSeriesSplit(n_splits=n_splits)
    locs = list(tscv.split(idx))
    splits = [(idx[loc[0]], idx[loc[1]]) for loc in locs]
    return splits


def truncate_index(idx: pd.Index, min_idx: Any, max_idx: Any) -> pd.Index:
    """
    Return subset of idx with values >= min_idx and < max_idx.
    """
    hpandas.dassert_strictly_increasing_index(idx)
    # TODO(*): PTask667: Consider using bisection to avoid linear scans.
    min_mask = idx >= min_idx
    max_mask = idx < max_idx
    mask = min_mask & max_mask
    hdbg.dassert_lte(1, mask.sum())
    return idx[mask]


def combine_indices(idxs: Iterable[pd.Index]) -> pd.Index:
    """
    Combine multiple indices into a single index for cross-validation splits.

    This is computed as the union of all the indices within the largest common
    interval.

    TODO(Paul): Consider supporting multiple behaviors with `mode`.
    """
    for idx in idxs:
        hpandas.dassert_strictly_increasing_index(idx)
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


def _compute_denominator_and_package(
    reduction: Union[float, np.ndarray],
    data: Union[pd.Series, pd.DataFrame],
    axis: Optional[float] = None,
) -> Union[float, pd.Series]:
    """
    Normalize and package `reduction` according to `axis` and `data` metadata.

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
        hdbg.dassert(not axis)
        return normalized
    hdbg.dassert_isinstance(normalized, np.ndarray)
    if axis == 0:
        return pd.Series(data=normalized, index=df.columns)
    if axis == 1:
        return pd.Series(data=normalized, index=df.index)
    raise ValueError("axis=`%s` but expected to be `0` or `1`!" % axis)
