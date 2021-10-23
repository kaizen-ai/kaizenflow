"""
Import as:

import core.signal_processing as csipro
"""

import collections
import functools
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pywt

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Correlation helpers
# #############################################################################


def correlate_with_lag(
    df: pd.DataFrame, lag: Union[int, List[int]]
) -> pd.DataFrame:
    """
    Combine cols of `df` with their lags and compute the correlation matrix.

    :param df: dataframe of numeric values
    :param lag: number of lags to apply or list of number of lags
    :return: correlation matrix with `(1 + len(lag)) * df.columns` columns
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    if isinstance(lag, int):
        lag = [lag]
    elif isinstance(lag, list):
        pass
    else:
        raise ValueError("Invalid `type(lag)`='%s'" % type(lag))
    lagged_dfs = [df]
    for lag_curr in lag:
        df_lagged = df.shift(lag_curr)
        df_lagged.columns = df_lagged.columns.astype(str) + f"_lag_{lag_curr}"
        lagged_dfs.append(df_lagged)
    merged_df = pd.concat(lagged_dfs, axis=1)
    return merged_df.corr()


def correlate_with_lagged_cumsum(
    df: pd.DataFrame,
    lag: int,
    y_vars: List[str],
    x_vars: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Compute correlation matrix of `df` cols and lagged cumulative sums.

    The flow is the following:
        - Compute cumulative sums of `y_vars` columns for `num_steps = lag`
        - Lag them so that `x_t` aligns with `y_{t+1} + ... + y{t+lag}`
        - Compute correlation of `df` columns (other than `y_vars`) and the
          lagged cumulative sums of `y_vars`

    This function can be applied to compute correlations between predictors and
    cumulative log returns.

    :param df: dataframe of numeric values
    :param lag: number of time points to shift the data by. Number of steps to
        compute rolling sum is `lag` too.
    :param y_vars: names of columns for which to compute cumulative sum
    :param x_vars: names of columns to correlate the `y_vars` with. If `None`,
        defaults to all columns except `y_vars`
    :return: correlation matrix of `(len(x_vars), len(y_vars))` shape
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_isinstance(y_vars, list)
    x_vars = x_vars or df.columns.difference(y_vars).tolist()
    hdbg.dassert_isinstance(x_vars, list)
    hdbg.dassert_lte(
        1,
        len(x_vars),
        "There are no columns to compute the correlation of cumulative "
        "returns with. ",
    )
    df = df[x_vars + y_vars].copy()
    cumsum_df = _compute_lagged_cumsum(df, lag, y_vars=y_vars)
    corr_df = cumsum_df.corr()
    y_cumsum_vars = cumsum_df.columns.difference(x_vars)
    return corr_df.loc[x_vars, y_cumsum_vars]


def _compute_lagged_cumsum(
    df: pd.DataFrame,
    lag: int,
    y_vars: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Compute lagged cumulative sum for selected columns.

    Align `x_t` with `y_{t+1} + ... + y{t+lag}`.

    :param df: dataframe of numeric values
    :param lag: number of time points to shift the data by. Number of steps to
        compute rolling sum is `lag`
    :param y_vars: names of columns for which to compute cumulative sum. If
        `None`, compute for all columns
    :return: dataframe with lagged cumulative sum columns
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    y_vars = y_vars or df.columns.tolist()
    hdbg.dassert_isinstance(y_vars, list)
    x_vars = df.columns.difference(y_vars)
    y = df[y_vars].copy()
    x = df[x_vars].copy()
    # Compute cumulative sum.
    y_cumsum = y.rolling(window=lag).sum()
    y_cumsum.rename(columns=lambda x: f"{x}_cumsum_{lag}", inplace=True)
    # Let's lag `y` so that `x_t` aligns with `y_{t+1} + ... + y{t+lag}`.
    y_cumsum_lagged = y_cumsum.shift(-lag)
    y_cumsum_lagged.rename(columns=lambda z: f"{z}_lag_{lag}", inplace=True)
    #
    merged_df = x.merge(y_cumsum_lagged, left_index=True, right_index=True)
    return merged_df


def calculate_inverse(
    df: pd.DataFrame,
    p_moment: Optional[Any] = None,
    info: Optional[collections.OrderedDict] = None,
) -> pd.DataFrame:
    """
    Calculate an inverse matrix.

    :param df: matrix to invert
    :param p_moment: order of the matrix norm as in `np.linalg.cond`
    :param info: dict with info to add the condition number to
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_eq(
        df.shape[0], df.shape[1], "Only square matrices are invertible."
    )
    hdbg.dassert(
        df.apply(lambda s: pd.to_numeric(s, errors="coerce").notnull()).all(
            axis=None
        ),
        "The matrix is not numeric.",
    )
    hdbg.dassert_ne(np.linalg.det(df), 0, "The matrix is non-invertible.")
    if info is not None:
        info["condition_number"] = np.linalg.cond(df, p_moment)
    return pd.DataFrame(np.linalg.inv(df), df.columns, df.index)


def calculate_pseudoinverse(
    df: pd.DataFrame,
    rcond: Optional[float] = 1e-15,
    hermitian: Optional[bool] = False,
    p_moment: Optional[Any] = None,
    info: Optional[collections.OrderedDict] = None,
) -> pd.DataFrame:
    """
    Calculate a pseudoinverse matrix.

    :param df: matrix to pseudo-invert
    :param rcond: cutoff for small singular values as in `np.linalg.pinv`
    :param hermitian: if True, `df` is assumed to be Hermitian
    :param p_moment: order of the matrix norm as in `np.linalg.cond`
    :param info: dict with info to add the condition number to
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert(
        df.apply(lambda s: pd.to_numeric(s, errors="coerce").notnull()).all(
            axis=None
        ),
        "The matrix is not numeric.",
    )
    if info is not None:
        info["condition_number"] = np.linalg.cond(df, p_moment)
    return pd.DataFrame(
        np.linalg.pinv(df, rcond=rcond, hermitian=hermitian), df.columns, df.index
    )


# #############################################################################
# Signal transformations
# #############################################################################


def compress_tails(
    signal: Union[pd.DataFrame, pd.Series], scale: float = 1
) -> Union[pd.DataFrame, pd.Series]:
    """
    Apply compression to data.

    :param signal: data
    :param scale: Divide data by scale and multiply compressed output by scale.
        Rescaling approximately preserves behavior in a neighborhood of the
        origin where the compression function is approximately linear.
    :return: compressed data
    """
    hdbg.dassert_lt(0, scale)
    return scale * np.tanh(signal / scale)


def get_symmetric_equisized_bins(
    signal: pd.Series, bin_size: float, zero_in_bin_interior: bool = False
) -> np.array:
    """
    Get bins of equal size, symmetric about zero, adapted to `signal`.

    :param bin_size: width of bin
    :param zero_in_bin_interior: Determines whether `0` is a bin edge or not.
        If in interior, it is placed in the center of the bin.
    :return: array of bin boundaries
    """
    # Remove +/- inf for the purpose of calculating max/min.
    finite_signal = signal.replace([-np.inf, np.inf], np.nan).dropna()
    # Determine minimum and maximum bin boundaries based on values of `signal`.
    # Make them symmetric for simplicity.
    left = np.floor(finite_signal.min() / bin_size).astype(int) - 1
    right = np.ceil(finite_signal.max() / bin_size).astype(int) + 1
    bin_boundary = bin_size * np.maximum(np.abs(left), np.abs(right))
    if zero_in_bin_interior:
        right_start = bin_size / 2
    else:
        right_start = 0
    right_bins = np.arange(right_start, bin_boundary, bin_size)
    # Reflect `right_bins` to get `left_bins`.
    if zero_in_bin_interior:
        left_bins = -np.flip(right_bins)
    else:
        left_bins = -np.flip(right_bins[1:])
    # Combine `left_bins` and `right_bin` into one bin.
    return np.append(left_bins, right_bins)


def digitize(signal: pd.Series, bins: np.array, right: bool = False) -> pd.Series:
    """
    Digitize (i.e., discretize) `signal` into `bins`.

    - In the output, bins are referenced with integers and are such that `0`
      always belongs to bin `0`
    - The bin-referencing convention is optimized for studying signals centered
      at zero (e.g., returns, z-scored features, etc.)
    - For bins of equal size, the bin-referencing convention makes it easy to
      map back from the digitized signal to numerical ranges given
        - the bin number
        - the bin size

    :param bins: array-like bin boundaries. Must include max and min `signal`
        values.
    :param right: same as in `np.digitize`
    :return: digitized signal
    """
    # From https://docs.scipy.org/doc/numpy/reference/generated/numpy.digitize.html
    # (v 1.17):
    # > If values in x are beyond the bounds of bins, 0 or len(bins) is
    # > returned as appropriate.
    digitized = np.digitize(signal, bins, right)
    # Center so that `0` belongs to bin "0"
    bin_containing_zero = np.digitize([0], bins, right)
    digitized -= bin_containing_zero
    # Convert to pd.Series, since `np.digitize` only returns an np.array.
    digitized_srs = pd.Series(
        data=digitized, index=signal.index, name=signal.name
    )
    return digitized_srs


def _wrap(signal: pd.Series, num_cols: int) -> pd.DataFrame:
    """
    Convert a 1-d series into a 2-d dataframe left-to-right top-to-bottom.

    :param num_cols: number of columns to use for wrapping
    """
    hdbg.dassert_isinstance(signal, pd.Series)
    hdbg.dassert_lte(1, num_cols)
    values = signal.values
    _LOG.debug("num values=%d", values.size)
    # Calculate number of rows that wrapped pd.DataFrame should have.
    num_rows = np.ceil(values.size / num_cols).astype(int)
    _LOG.debug("num_rows=%d", num_rows)
    # Add padding, since numpy's `reshape` requires element counts to match
    # exactly.
    pad_size = num_rows * num_cols - values.size
    _LOG.debug("pad_size=%d", pad_size)
    padding = np.full(pad_size, np.nan)
    padded_values = np.append(values, padding)
    #
    wrapped = padded_values.reshape(num_rows, num_cols)
    return pd.DataFrame(wrapped)


def _unwrap(
    df: pd.DataFrame, idx: pd.Index, name: Optional[Any] = None
) -> pd.Series:
    """
    Undo `_wrap`.

    We allow `index.size` to be less than nrows * ncols of `df`, in which case
    values are truncated from the end of the unwrapped dataframe.

    :param idx: index of series provided to `_wrap` call
    """
    _LOG.debug("df.shape=%s", df.shape)
    values = df.values.flatten()
    pad_size = values.size - idx.size
    _LOG.debug("pad_size=%d", pad_size)
    if pad_size > 0:
        data = values[:-pad_size]
    else:
        data = values
    unwrapped = pd.Series(data=data, index=idx, name=name)
    return unwrapped


def skip_apply_func(
    signal: pd.DataFrame,
    skip_size: int,
    func: Callable[[pd.Series], pd.DataFrame],
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Apply `func` to each col of `signal` after a wrap, then unwrap and merge.

    :param skip_size: num_cols used for wrapping each col of `signal`
    :param kwargs: forwarded to `func`
    """
    cols = {}
    for col in signal.columns:
        wrapped = _wrap(signal[col], skip_size)
        funced = func(wrapped, **kwargs)
        unwrapped = _unwrap(funced, signal.index, col)
        cols[col] = unwrapped
    df = pd.DataFrame.from_dict(cols)
    return df


# TODO(Paul): Add test coverage.
def sign_normalize(
    signal: Union[pd.DataFrame, pd.Series],
    atol: float = 0,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Normalize nonzero values to +/- 1 according to sign.

    :param signal: series or 1-dimensional dataframe
    :param atol: values < atol are forced to zero
    :return: series/1-d dataframe with finite values of either -1, 0, or 1
    """
    # Convert to series before performing calculations.
    convert_to_frame = False
    if isinstance(signal, pd.DataFrame):
        signal = signal.squeeze()
        convert_to_frame = True
    hdbg.dassert_isinstance(
        signal,
        pd.Series,
        msg="Only series and 1-dimensional dataframes are admissible",
    )
    # Force small values to zero.
    atol_mask = signal.abs() < atol
    normalized_signal = signal.copy()
    normalized_signal.loc[atol_mask] = 0
    # Mask out zeros when rescaling nonzero values.
    zero_mask = normalized_signal == 0
    normalized_signal = normalized_signal.copy()
    normalized_signal.loc[~zero_mask] /= normalized_signal.loc[~zero_mask].abs()
    if convert_to_frame:
        normalized_signal = normalized_signal.to_frame()
    return normalized_signal


# TODO(Paul): Add test coverage.
def normalize(
    signal: pd.Series,
) -> pd.Series:
    """
    Normalize `signal` by dividing it by its l2 norm.
    """
    hdbg.dassert_isinstance(signal, pd.Series)
    hdbg.dassert(
        not signal.isna().any(),
        msg="NaNs detected at %s" % signal[signal.isna()].index,
    )
    norm = np.linalg.norm(signal)
    _LOG.debug("l2 norm=%f", norm)
    normalized = signal / norm
    return normalized


def split_positive_and_negative_parts(
    signal: Union[pd.Series, pd.DataFrame],
) -> pd.DataFrame:
    """
    Split `signal` into max(signal, 0) and max(-signal, 0).
    """
    if isinstance(signal, pd.DataFrame):
        hdbg.dassert_eq(len(signal.columns), 1)
        signal = signal.squeeze()
    hdbg.dassert_isinstance(signal, pd.Series)
    positive = ((signal + signal.abs()) / 2).rename("positive")
    negative = ((signal.abs() - signal) / 2).rename("negative")
    df = pd.concat([positive, negative], axis=1)
    return df


# #############################################################################
# EMAs and derived kernels
# #############################################################################


def calculate_tau_from_com(com: float) -> Union[float, np.float]:
    """
    Transform center-of-mass (com) into tau parameter.

    This is the function inverse of `calculate_com_from_tau`.
    """
    hdbg.dassert_lt(0, com)
    return 1.0 / np.log(1 + 1.0 / com)


def calculate_com_from_tau(tau: float) -> Union[float, np.float]:
    """
    Transform tau parameter into center-of-mass (com).

    We use the tau parameter for kernels (as in Dacorogna, et al), but for the
    compute_ema operator want to take advantage of pandas' implementation, which uses
    different parameterizations. We adopt `com` because
        - It is almost equal to `tau`
        - We have used it historically

    :param tau: parameter used in (continuous) compute_ema and compute_ema-derived kernels. For
        typical ranges it is approximately but not exactly equal to the
        center-of-mass (com) associated with an compute_ema kernel.
    :return: com
    """
    hdbg.dassert_lt(0, tau)
    return 1.0 / (np.exp(1.0 / tau) - 1)


def compute_ema(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int,
    depth: int = 1,
) -> Union[pd.DataFrame, pd.Series]:
    r"""Implement iterated EMA operator (e.g., see 3.3.6 of Dacorogna, et al).

    depth=1 corresponds to a single application of exponential smoothing.

    Greater depth tempers impulse response, introducing a phase lag.

    Dacorogna use the convention $\text{compute_ema}(t) = \exp(-t / \tau) / \tau$.

    If $s_n = \lambda x_n + (1 - \lambda) s_{n - 1}$, where $s_n$ is the compute_ema
    output, then $1 - \lambda = \exp(-1 / \tau)$. Now
    $\lambda = 1 / (1 + \text{com})$, and rearranging gives
    $\log(1 + 1 / \text{com}) = 1 / \tau$. Expanding in a Taylor series
    leads to $\tau \approx \text{com}$.

    The kernel for an compute_ema of depth $n$ is
    $(1 / (n - 1)!) (t / \tau)^{n - 1} \exp^{-t / \tau} / \tau$.

    Arbitrary kernels can be approximated by a combination of iterated emas.

    For an iterated compute_ema with given tau and depth n, we have
      - range = n \tau
      - <t^2> = n(n + 1) \tau^2
      - width = \sqrt{n} \tau
      - aspect ratio = \sqrt{1 + 1 / n}
    """
    hdbg.dassert_isinstance(depth, int)
    hdbg.dassert_lte(1, depth)
    hdbg.dassert_lt(0, tau)
    _LOG.debug("Calculating iterated ema of depth %i", depth)
    _LOG.debug("range = %0.2f", depth * tau)
    _LOG.debug("<t^2>^{1/2} = %0.2f", np.sqrt(depth * (depth + 1)) * tau)
    _LOG.debug("width = %0.2f", np.sqrt(depth) * tau)
    _LOG.debug("aspect ratio = %0.2f", np.sqrt(1 + 1.0 / depth))
    _LOG.debug("tau = %0.2f", tau)
    com = calculate_com_from_tau(tau)
    _LOG.debug("com = %0.2f", com)
    signal_hat = signal.copy()
    for _ in range(0, depth):
        signal_hat = signal_hat.ewm(
            com=com, min_periods=min_periods, adjust=True, ignore_na=False, axis=0
        ).mean()
    return signal_hat


def compute_smooth_derivative(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int,
    scaling: int = 1,
    order: int = 1,
) -> Union[pd.DataFrame, pd.Series]:
    r"""Compute a "low-noise" differential operator.

    'Low-noise' differential operator as in 3.3.9 of Dacorogna, et al.

    - Computes difference of around time "now" over a time interval \tau_1 and
      an average around time "now - \tau" over a time interval \tau_2
    - Here, \tau_1, \tau_2 are taken to be approximately `tau`/ 2

    The normalization factors are chosen so that
      - the differential of a constant is zero
      - the differential (`scaling` = 0) of `t` is approximately `tau`
      - the derivative (`order` = 1) of `t` is approximately 1

    The `scaling` parameter refers to the exponential weighting of inverse
    tau.

    The `order` parameter refers to the number of times the
    compute_smooth_derivative operator is applied to the original signal.
    """
    hdbg.dassert_isinstance(order, int)
    hdbg.dassert_lte(0, order)
    gamma = 1.22208
    beta = 0.65
    alpha = 1.0 / (gamma * (8 * beta - 3))
    _LOG.debug("alpha = %0.2f", alpha)
    tau1 = alpha * tau
    _LOG.debug("tau1 = %0.2f", tau1)
    tau2 = alpha * beta * tau
    _LOG.debug("tau2 = %0.2f", tau2)

    def order_one(
        signal: Union[pd.DataFrame, pd.Series]
    ) -> Union[pd.DataFrame, pd.Series]:
        s1 = compute_ema(signal, tau1, min_periods, 1)
        s2 = compute_ema(signal, tau1, min_periods, 2)
        s3 = -2.0 * compute_ema(signal, tau2, min_periods, 4)
        differential = gamma * (s1 + s2 + s3)
        if scaling == 0:
            return differential
        return differential / (tau ** scaling)

    signal_diff = signal.copy()
    for _ in range(0, order):
        signal_diff = order_one(signal_diff)
    return signal_diff


def compute_smooth_moving_average(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
) -> Union[pd.DataFrame, pd.Series]:
    """Implement moving average operator defined in terms of iterated compute_ema's.
    Choosing min_depth > 1 results in a lagged operator.
    Choosing min_depth = max_depth = 1 reduces to a single compute_ema.

    Abrupt impulse response that tapers off smoothly like a sigmoid
    (hence smoother than an equally-weighted moving average).

    For min_depth = 1 and large max_depth, the series is approximately
    constant for t << 2 * range_. In particular, when max_depth >= 5,
    the kernels are more rectangular than compute_ema-like.
    """
    hdbg.dassert_isinstance(min_depth, int)
    hdbg.dassert_isinstance(max_depth, int)
    hdbg.dassert_lte(1, min_depth)
    hdbg.dassert_lte(min_depth, max_depth)
    range_ = tau * (min_depth + max_depth) / 2.0
    _LOG.debug("Range = %0.2f", range_)
    ema_eval = functools.partial(compute_ema, signal, tau, min_periods)
    denom = float(max_depth - min_depth + 1)
    # Not the most efficient implementation, but follows 3.56 of Dacorogna
    # directly.
    return sum(map(ema_eval, range(min_depth, max_depth + 1))) / denom


def extract_smooth_moving_average_weights(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_depth: int = 1,
    max_depth: int = 1,
    index_location: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Return present and historical weights used in SMA up to `index_location`.

    This can be used in isolation to inspect SMA weights, or can be used on
    data to, e.g., generate training data weights.

    TODO(Paul): Consider generalizing this to also work with other filters.

    :param signal: data that provides an index (for reindexing). No column
        values used.
    :param tau: as in `compute_smooth_moving_average()`
    :param min_depth: as in `compute_smooth_moving_average()`
    :param max_depth: as in `compute_smooth_moving_average()`
    :param index_location: current and latest value to be considered operated
        upon by the smooth moving average (e.g., the last in-sample index). If
        `None`, then use the last index location of `signal`.
    :return: dataframe with two columns of weights:
        1. absolute weights (e.g., weights sum to 1)
        2. relative weights (weight at `index_location` is equal to `1`, and
           prior weights are expressed relative to this value
    """
    hdbg.dassert_lt(0, tau)
    range_ = tau * (min_depth + max_depth) / 2.0
    warmup_length = int(np.round(10 * range_))
    weights = extract_filter_weights(
        signal,
        func=compute_smooth_moving_average,
        func_kwargs={"tau": tau, "min_depth": min_depth, "max_depth": max_depth},
        warmup_length=warmup_length,
        index_location=index_location,
    )
    return weights


def extract_filter_weights(
    signal: Union[pd.DataFrame, pd.Series],
    func: Callable[[pd.Series], pd.Series],
    func_kwargs: Dict[str, Any],
    warmup_length: int,
    index_location: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Return present and historical weights used in SMA up to `index_location`.

    This can be used in isolation to inspect SMA weights, or can be used on
    data to, e.g., generate training data weights.

    TODO(Paul): Consider generalizing this to also work with other filters.

    :param signal: data that provides an index (for reindexing). No column
        values used.
    :param func: a function that transforms a series into a series
    :param func_kwargs: kwargs to forward to `func`, e.g., smoothing parameters
    :param warmup_length: a lower bound on the number of points required for
        properly initializing `func` in generating weights (e.g., if `func` is
        an EWMA)
    :param index_location: current and latest value to be considered operated
        upon by the smooth moving average (e.g., the last in-sample index). If
        `None`, then use the last index location of `signal`.
    :return: dataframe with two columns of weights:
        1. absolute weights (e.g., weights sum to 1)
        2. relative weights (weight at `index_location` is equal to `1`, and
           prior weights are expressed relative to this value
    """
    idx = signal.index
    hdbg.dassert_isinstance(idx, pd.Index)
    hdbg.dassert(not idx.empty, msg="`signal.index` must be nonempty.")
    index_location = index_location or idx[-1]
    if index_location > idx[-1]:
        _LOG.warning(
            "Requested `index_location` is out-of-range. "
            "Proceeding with last `signal.index` location instead."
        )
        index_location = idx[-1]
    hdbg.dassert_in(
        index_location,
        idx,
        msg="`index_location` must be a member of `signal.index`",
    )
    hdbg.dassert_lt(0, warmup_length)
    # Build a step series.
    # - This is a sequence of ones followed by a sequence of zeros
    # - The length of the ones series is determined by `tau` and is used for
    #   warm-up
    # - The length of the zeros is at least as long as the length of the
    #   weight series implicitly asked for by the caller. If this is less than
    #   the warm-up length, then we extend the zeros so that we can calculate
    #   reliable absolute weights
    desired_length = signal.loc[:index_location].shape[0]
    ones = pd.Series(index=range(0, warmup_length), data=1)
    length = max(desired_length, warmup_length)
    zeros = pd.Series(index=range(warmup_length, warmup_length + length), data=0)
    step = pd.concat([ones, zeros], axis=0)
    # Apply the smooth moving average function to the step function.
    filtered_step = func(step, **func_kwargs)
    # Drop the warm-up ones from the smoothed series.
    filtered_step = filtered_step.iloc[warmup_length - 1 :]
    filtered_step.name = "relative_weight"
    # Calculate absolute weights.
    absolute_weights = (filtered_step / filtered_step.sum()).rename(
        "absolute_weight"
    )
    # Build a `weights` dataframe of relative and absolute kernel weights.
    weights = pd.concat([filtered_step, absolute_weights], axis=1).reset_index(
        drop=True
    )
    # Truncate to `desired_length`, determined by `signal.index` and
    # `index_location`.
    weights = weights.iloc[:desired_length]
    # Reverse the series (because the weights apply to historical
    # observations).
    weights = weights.iloc[::-1].reset_index(drop=True)
    # Index and align the weights so that they terminate at `index_location`.
    weights.index = signal.loc[:index_location].index
    # Extend `weights` with NaNs if necessary.
    return weights.reindex(idx)


# #############################################################################
# Rolling moments, norms, z-scoring, demeaning, etc.
# #############################################################################


def compute_rolling_moment(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    return compute_smooth_moving_average(
        np.abs(signal) ** p_moment, tau, min_periods, min_depth, max_depth
    )


def compute_rolling_norm(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
    delay: float = 0,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Implement smooth moving average norm (when p_moment >= 1).

    Moving average corresponds to compute_ema when min_depth = max_depth = 1.
    """
    hdbg.dassert_lte(0, delay, "Requested delay=%i is non-causal.", delay)
    signal = signal.shift(delay)
    signal_p = compute_rolling_moment(
        signal, tau, min_periods, min_depth, max_depth, p_moment
    )
    return signal_p ** (1.0 / p_moment)


def compute_rolling_var(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Implement smooth moving average central moment.

    Moving average corresponds to compute_ema when min_depth = max_depth = 1.
    """
    signal_ma = compute_smooth_moving_average(
        signal, tau, min_periods, min_depth, max_depth
    )
    return compute_rolling_moment(
        signal - signal_ma, tau, min_periods, min_depth, max_depth, p_moment
    )


def compute_rolling_std(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Implement normalized smooth moving average central moment.

    Moving average corresponds to compute_ema when min_depth = max_depth = 1.
    """
    signal_tmp = compute_rolling_var(
        signal, tau, min_periods, min_depth, max_depth, p_moment
    )
    return signal_tmp ** (1.0 / p_moment)


def compute_rolling_demean(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Demean signal on a rolling basis with compute_smooth_moving_average.
    """
    signal_ma = compute_smooth_moving_average(
        signal, tau, min_periods, min_depth, max_depth
    )
    return signal - signal_ma


def compute_rolling_zscore(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
    demean: bool = True,
    delay: int = 0,
    atol: float = 0,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Z-score using compute_smooth_moving_average and compute_rolling_std.

    If delay > 0, then pay special attention to 0 and NaN handling to avoid
    extreme values.

    Moving average corresponds to compute_ema when min_depth = max_depth = 1.

    If denominator.abs() <= atol, Z-score value is set to np.nan in order to
    avoid extreme value spikes.

    TODO(Paul): determine whether signal == signal.shift(0) always.
    """
    if demean:
        # Equivalent to invoking compute_rolling_demean and compute_rolling_std, but this way
        # we avoid calculating signal_ma twice.
        signal_ma = compute_smooth_moving_average(
            signal, tau, min_periods, min_depth, max_depth
        )
        signal_std = compute_rolling_norm(
            signal - signal_ma, tau, min_periods, min_depth, max_depth, p_moment
        )
        numerator = signal - signal_ma.shift(delay)
    else:
        signal_std = compute_rolling_norm(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        numerator = signal
    denominator = signal_std.shift(delay)
    denominator[denominator.abs() <= atol] = np.nan
    ret = numerator / denominator
    return ret


def compute_rolling_skew(
    signal: Union[pd.DataFrame, pd.Series],
    tau_z: float,
    tau_s: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Smooth moving average skew of z-scored signal.
    """
    z_signal = compute_rolling_zscore(
        signal, tau_z, min_periods, min_depth, max_depth, p_moment
    )
    skew = compute_smooth_moving_average(
        z_signal ** 3, tau_s, min_periods, min_depth, max_depth
    )
    return skew


def compute_rolling_kurtosis(
    signal: Union[pd.DataFrame, pd.Series],
    tau_z: float,
    tau_s: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Smooth moving average kurtosis of z-scored signal.
    """
    z_signal = compute_rolling_zscore(
        signal, tau_z, min_periods, min_depth, max_depth, p_moment
    )
    kurt = compute_smooth_moving_average(
        z_signal ** 4, tau_s, min_periods, min_depth, max_depth
    )
    return kurt


def compute_centered_gaussian_log_likelihood(
    df: pd.DataFrame,
    observation_col: str,
    variance_col: str,
    square_variance_col: bool = False,
    variance_shifts: int = 0,
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """
    Return the log-likelihoods of independent draws from centered Gaussians.

    A higher log-likelihood score means that the model of independent
    Gaussian draws with given variances is a better fit.

    The log-likelihood of the series of observations may be obtained by
    summing the individual log-likelihood values.

    :param df: dataframe with float observation and variance columns
    :param observation_col: name of column containing observations
    :param variance_col: name of column containing variances
    :square_variance_col: if `True`, square the values in `variance_col`
        (use this if the column contains standard deviations)
    :variance_shifts: number of shifts to apply to `variance_col` prior to
        calculating log-likelihood. Use this if `variance_col` contains forward
        predictions.
    :prefix: prefix to add to name of output series
    :return: dataframe of log-likelihoods and adjusted observations
    """
    prefix = prefix or ""
    hdbg.dassert_isinstance(df, pd.DataFrame)
    # Extract observations and variance, with optional shift applied.
    obs = df[observation_col]
    var = df[variance_col].shift(variance_shifts)
    hdbg.dassert(not (var <= 0).any(), msg="Variance values must be positive.")
    if square_variance_col:
        var = np.square(var)
    # Restrict to relevant data and drop any rows with NaNs.
    idx = pd.concat([obs, var], axis=1).dropna().index
    obs = obs.loc[idx]
    var = var.loc[idx]
    # Ensure that there is at least one observation.
    n_obs = idx.size
    _LOG.debug("Number of non-NaN observations=%i", n_obs)
    hdbg.dassert_lt(0, n_obs)
    # Perform log-likelihood calculation.
    # This term only depends upon the presence of an observation. We preserve
    # it here to facilitate comparisons across series with different numbers of
    # observations.
    constant_term = -0.5 * np.log(2 * np.pi)
    # This term depends upon the observation values and variances.
    data_term = -0.5 * (np.log(var) + np.square(obs).divide(var))
    log_likelihoods = constant_term + data_term
    log_likelihoods.name = prefix + "log_likelihood"
    # Compute observations normalized by standard deviations.
    adj_obs = obs.divide(np.sqrt(var))
    adj_obs.name = prefix + "normalized_observations"
    # Construct output dataframe.
    df_out = pd.concat([adj_obs, log_likelihoods], axis=1)
    return df_out


# #############################################################################
# Rolling Sharpe ratio
# #############################################################################


def compute_rolling_annualized_sharpe_ratio(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    points_per_year: float,
    min_periods: int = 2,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Compute rolling annualized Sharpe ratio and standard error.

    The standard error adjustment uses the range of the smooth moving
    average kernel as an estimate of the "number of data points" used in
    the calculation of the Sharpe ratio.
    """
    sr = compute_rolling_sharpe_ratio(
        signal, tau, min_periods, min_depth, max_depth, p_moment
    )
    # TODO(*): May need to rescale denominator by a constant.
    se_sr = np.sqrt((1 + (sr ** 2) / 2) / (tau * max_depth))
    rescaled_sr = np.sqrt(points_per_year) * sr
    rescaled_se_sr = np.sqrt(points_per_year) * se_sr
    df = pd.DataFrame(index=signal.index)
    df["annualized_SR"] = rescaled_sr
    df["annualized_SE(SR)"] = rescaled_se_sr
    return df


def compute_rolling_sharpe_ratio(
    signal: Union[pd.DataFrame, pd.Series],
    tau: float,
    min_periods: int = 2,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Sharpe ratio using compute_smooth_moving_average and compute_rolling_std.
    """
    signal_ma = compute_smooth_moving_average(
        signal, tau, min_periods, min_depth, max_depth
    )
    # Use `zero` as the mean in the standard deviation calculation.
    signal_std = compute_rolling_norm(
        signal, tau, min_periods, min_depth, max_depth, p_moment
    )
    return signal_ma / signal_std


# #############################################################################
# Rolling correlation functions
# #############################################################################


# TODO(Paul): Change the interface so that the two series are cols of a df.
def compute_rolling_cov(
    srs1: Union[pd.DataFrame, pd.Series],
    srs2: Union[pd.DataFrame, pd.Series],
    tau: float,
    demean: bool = True,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Smooth moving covariance.
    """
    if demean:
        srs1_adj = srs1 - compute_smooth_moving_average(
            srs1, tau, min_periods, min_depth, max_depth
        )
        srs2_adj = srs2 - compute_smooth_moving_average(
            srs2, tau, min_periods, min_depth, max_depth
        )
    else:
        srs1_adj = srs1
        srs2_adj = srs2
    smooth_prod = compute_smooth_moving_average(
        srs1_adj.multiply(srs2_adj), tau, min_periods, min_depth, max_depth
    )
    return smooth_prod


def compute_rolling_corr(
    srs1: Union[pd.DataFrame, pd.Series],
    srs2: Union[pd.DataFrame, pd.Series],
    tau: float,
    demean: bool = True,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Smooth moving correlation.
    """
    if demean:
        srs1_adj = srs1 - compute_smooth_moving_average(
            srs1, tau, min_periods, min_depth, max_depth
        )
        srs2_adj = srs2 - compute_smooth_moving_average(
            srs2, tau, min_periods, min_depth, max_depth
        )
    else:
        srs1_adj = srs1
        srs2_adj = srs2
    smooth_prod = compute_smooth_moving_average(
        srs1_adj.multiply(srs2_adj), tau, min_periods, min_depth, max_depth
    )
    srs1_std = compute_rolling_norm(
        srs1_adj, tau, min_periods, min_depth, max_depth, p_moment
    )
    srs2_std = compute_rolling_norm(
        srs2_adj, tau, min_periods, min_depth, max_depth, p_moment
    )
    return smooth_prod / (srs1_std * srs2_std)


def compute_rolling_zcorr(
    srs1: Union[pd.DataFrame, pd.Series],
    srs2: Union[pd.DataFrame, pd.Series],
    tau: float,
    demean: bool = True,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Z-score srs1, srs2 then calculate moving average of product.

    Not guaranteed to lie in [-1, 1], but bilinear in the z-scored
    variables.
    """
    if demean:
        z_srs1 = compute_rolling_zscore(
            srs1, tau, min_periods, min_depth, max_depth, p_moment
        )
        z_srs2 = compute_rolling_zscore(
            srs2, tau, min_periods, min_depth, max_depth, p_moment
        )
    else:
        z_srs1 = srs1 / compute_rolling_norm(
            srs1, tau, min_periods, min_depth, max_depth, p_moment
        )
        z_srs2 = srs2 / compute_rolling_norm(
            srs2, tau, min_periods, min_depth, max_depth, p_moment
        )
    return compute_smooth_moving_average(
        z_srs1.multiply(z_srs2), tau, min_periods, min_depth, max_depth
    )


# #############################################################################
# Outlier handling
# #############################################################################


def process_outliers(
    srs: pd.Series,
    mode: str,
    lower_quantile: float,
    upper_quantile: Optional[float] = None,
    window: Optional[int] = None,
    min_periods: Optional[int] = None,
    info: Optional[dict] = None,
) -> pd.Series:
    """
    Process outliers in different ways given lower / upper quantiles.

    Default behavior:
      - If `window` is `None`, set `window` to series length
        - This works like an expanding window (we always look at the full
          history, except for anything burned by `min_periods`)
      - If `min_periods` is `None` and `window` is `None`, set `min_periods` to
        `0`
        - Like an expanding window with no data burned
      - If `min_periods` is `None` and `window` is not `None`, set `min_periods`
        to `window`
        - This is a sliding window with leading data burned so that every
          estimate uses a full window's worth of data

    Note:
      - If `window` is set to `None` according to these conventions (i.e., we
        are in an "expanding window" mode), then outlier effects are never
        "forgotten" and the processing of the data can depend strongly upon
        where the series starts
      - For this reason, it is suggested that `window` be set to a finite value
        adapted to the data/frequency

    :param srs: pd.Series to process
    :param lower_quantile: lower quantile (in range [0, 1]) of the values to keep
        The interval of data kept without any changes is [lower, upper]. In other
        terms the outliers with quantiles strictly smaller and larger than the
        respective bounds are processed.
    :param upper_quantile: upper quantile with the same semantic as
        lower_quantile. If `None`, the quantile symmetric of the lower quantile
        with respect to 0.5 is taken. E.g., an upper quantile equal to 0.7 is
        taken for a lower_quantile = 0.3
    :param window: rolling window size
    :param min_periods: minimum number of observations in window required to
        calculate the quantiles. The first `min_periods` values will not be
        processed. If `None`, defaults to `window`.
    :param mode: it can be "winsorize", "set_to_nan", "set_to_zero"
    :param info: empty dict-like object that this function will populate with
        statistics about the performed operation

    :return: transformed series with the same number of elements as the input
        series. The operation is not in place.
    """
    # Check parameters.
    hdbg.dassert_isinstance(srs, pd.Series)
    hdbg.dassert_lte(0.0, lower_quantile)
    if upper_quantile is None:
        upper_quantile = 1.0 - lower_quantile
    hdbg.dassert_lte(lower_quantile, upper_quantile)
    hdbg.dassert_lte(upper_quantile, 1.0)
    # Process default `min_periods` and `window` parameters.
    if min_periods is None:
        if window is None:
            min_periods = 0
        else:
            min_periods = window
    if window is None:
        window = srs.shape[0]
    if window < 30:
        _LOG.warning("`window`=`%s` < `30`", window)
    if min_periods > window:
        _LOG.warning("`min_periods`=`%s` > `window`=`%s`", min_periods, window)
    # Compute bounds.
    l_bound = srs.rolling(window, min_periods=min_periods, center=False).quantile(
        lower_quantile
    )
    u_bound = srs.rolling(window, min_periods=min_periods, center=False).quantile(
        upper_quantile
    )
    _LOG.debug(
        "Removing outliers in [%s, %s] with mode=%s",
        lower_quantile,
        upper_quantile,
        mode,
    )
    # Compute stats.
    if info is not None:
        hdbg.dassert_isinstance(info, dict)
        # Dictionary should be empty.
        hdbg.dassert(not info)
        info["series_name"] = srs.name
        info["num_elems_before"] = len(srs)
        info["num_nans_before"] = np.isnan(srs).sum()
        info["num_infs_before"] = np.isinf(srs).sum()
        info["quantiles"] = (lower_quantile, upper_quantile)
        info["mode"] = mode
    #
    srs = srs.copy()
    # Here we implement the functions instead of using library functions (e.g,
    # `scipy.stats.mstats.winsorize`) since we want to compute some statistics
    # that are not readily available from the library function.
    l_mask = srs < l_bound
    u_mask = u_bound < srs
    if mode == "winsorize":
        # Assign the outliers to the value of the bounds.
        srs[l_mask] = l_bound[l_mask]
        srs[u_mask] = u_bound[u_mask]
    else:
        mask = u_mask | l_mask
        if mode == "set_to_nan":
            srs[mask] = np.nan
        elif mode == "set_to_zero":
            srs[mask] = 0.0
        else:
            hdbg.dfatal("Invalid mode='%s'" % mode)
    # Append more the stats.
    if info is not None:
        info["bounds"] = pd.DataFrame({"l_bound": l_bound, "u_bound": u_bound})
        num_removed = l_mask.sum() + u_mask.sum()
        info["num_elems_removed"] = num_removed
        info["num_elems_after"] = (
            info["num_elems_before"] - info["num_elems_removed"]
        )
        info["percentage_removed"] = (
            100.0 * info["num_elems_removed"] / info["num_elems_before"]
        )
        info["num_nans_after"] = np.isnan(srs).sum()
        info["num_infs_after"] = np.isinf(srs).sum()
    return srs


def process_outlier_df(
    df: pd.DataFrame,
    mode: str,
    lower_quantile: float,
    upper_quantile: Optional[float] = None,
    window: Optional[int] = None,
    min_periods: Optional[int] = None,
    info: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Extend `process_outliers` to dataframes.

    TODO(*): Revisit this with a decorator approach:
    https://github.com/.../.../issues/568
    """
    if info is not None:
        hdbg.dassert_isinstance(info, dict)
        # Dictionary should be empty.
        hdbg.dassert(not info)
    cols = {}
    for col in df.columns:
        if info is not None:
            maybe_stats: Optional[Dict[str, Any]] = {}
        else:
            maybe_stats = None
        srs = process_outliers(
            df[col],
            mode,
            lower_quantile,
            upper_quantile=upper_quantile,
            window=window,
            min_periods=min_periods,
            info=maybe_stats,
        )
        cols[col] = srs
        if info is not None:
            info[col] = maybe_stats
    ret = pd.DataFrame.from_dict(cols)
    # Check that the columns are the same. We don't use dassert_eq because of
    # #665.
    hdbg.dassert(
        all(df.columns == ret.columns),
        "Columns are different:\ndf.columns=%s\nret.columns=%s",
        str(df.columns),
        str(ret.columns),
    )
    return ret


def process_nonfinite(
    srs: pd.Series,
    remove_nan: bool = True,
    remove_inf: bool = True,
    info: Optional[dict] = None,
) -> pd.Series:
    """
    Remove infinite and NaN values according to the parameters.

    :param srs: pd.Series to process
    :param remove_nan: remove NaN values if True and keep if False
    :param remove_inf: remove infinite values if True and keep if False
    :param info: empty dict-like object that this function will populate with
        statistics about how many items were removed
    :return: transformed copy of the input series
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    nan_mask = np.isnan(srs)
    inf_mask = np.isinf(srs)
    nan_inf_mask = nan_mask | inf_mask
    # Make a copy of input that will be processed
    if remove_nan & remove_inf:
        res = srs[~nan_inf_mask].copy()
    elif remove_nan & ~remove_inf:
        res = srs[~nan_mask].copy()
    elif ~remove_nan & remove_inf:
        res = srs[~inf_mask].copy()
    else:
        res = srs.copy()
    if info is not None:
        hdbg.dassert_isinstance(info, dict)
        # Dictionary should be empty.
        hdbg.dassert(not info)
        info["series_name"] = srs.name
        info["num_elems_before"] = len(srs)
        info["num_nans_before"] = np.isnan(srs).sum()
        info["num_infs_before"] = np.isinf(srs).sum()
        info["num_elems_removed"] = len(srs) - len(res)
        info["num_nans_removed"] = info["num_nans_before"] - np.isnan(res).sum()
        info["num_infs_removed"] = info["num_infs_before"] - np.isinf(res).sum()
        info["percentage_elems_removed"] = (
            100.0 * info["num_elems_removed"] / info["num_elems_before"]
        )
    return res


# #############################################################################
# Incremental PCA
# #############################################################################


def compute_ipca(
    df: pd.DataFrame,
    num_pc: int,
    tau: float,
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """
    Incremental PCA.

    The dataframe should already be centered.

    https://ieeexplore.ieee.org/document/1217609
    https://www.cse.msu.edu/~weng/research/CCIPCApami.pdf

    :param num_pc: number of principal components to calculate
    :param tau: parameter used in (continuous) compute_ema and compute_ema-derived kernels. For
        typical ranges it is approximately but not exactly equal to the
        center-of-mass (com) associated with an compute_ema kernel.
    :return:
      - df of eigenvalue series (col 0 correspond to max eigenvalue, etc.).
      - list of dfs of unit eigenvectors (0 indexes df eigenvectors
        corresponding to max eigenvalue, etc.).
    """
    hdbg.dassert_isinstance(
        num_pc, int, msg="Specify an integral number of principal components."
    )
    hdbg.dassert_lt(
        num_pc,
        df.shape[0],
        msg="Number of time steps should exceed number of principal components.",
    )
    hdbg.dassert_lte(
        num_pc,
        df.shape[1],
        msg="Dimension should be greater than or equal to the number of principal components.",
    )
    hdbg.dassert_lt(0, tau)
    com = calculate_com_from_tau(tau)
    alpha = 1.0 / (com + 1.0)
    _LOG.debug("com = %0.2f", com)
    _LOG.debug("alpha = %0.2f", alpha)
    # TODO(Paul): Consider requiring that the caller do this instead.
    # Fill NaNs with zero.
    df.fillna(0, inplace=True)
    lambdas: Dict[int, list] = {k: [] for k in range(num_pc)}
    # V's are eigenvectors with norm equal to corresponding eigenvalue.
    vs: Dict[int, list] = {k: [] for k in range(num_pc)}
    unit_eigenvecs: Dict[int, list] = {k: [] for k in range(num_pc)}
    step = 0
    for n in df.index:
        # Initialize u(n).
        u = df.loc[n].copy()
        for i in range(min(num_pc, step + 1)):
            # Initialize ith eigenvector.
            if i == step:
                v = u.copy()
                if np.linalg.norm(v):
                    _LOG.debug("Initializing eigenvector %s...", i)
                    step += 1
            else:
                # Main update step for eigenvector i.
                u, v = _compute_ipca_step(u, vs[i][-1], alpha)
            # Bookkeeping.
            v.name = n
            vs[i].append(v)
            norm = np.linalg.norm(v)
            lambdas[i].append(norm)
            unit_eigenvecs[i].append(v / norm)
    _LOG.debug("Completed %s steps of incremental PCA.", len(df))
    # Convert lambda dict of lists to list of series.
    # Convert unit_eigenvecs dict of lists to list of dataframes.
    lambdas_srs = []
    unit_eigenvec_dfs = []
    for i in range(num_pc):
        lambdas_srs.append(
            pd.Series(index=df.index[-len(lambdas[i]) :], data=lambdas[i])
        )
        unit_eigenvec_dfs.append(pd.concat(unit_eigenvecs[i], axis=1).transpose())
    lambda_df = pd.concat(lambdas_srs, axis=1)
    return lambda_df, unit_eigenvec_dfs


def _compute_ipca_step(
    u: pd.Series, v: pd.Series, alpha: float
) -> Tuple[pd.Series, pd.Series]:
    """
    Single step of incremental PCA.

    At each point, the norm of v is the eigenvalue estimate (for the component
    to which u and v refer).

    :param u: residualized observation for step n, component i
    :param v: unnormalized eigenvector estimate for step n - 1, component i
    :param alpha: compute_ema-type weight (choose in [0, 1] and typically < 0.5)

    :return: (u_next, v_next), where
      * u_next is residualized observation for step n, component i + 1
      * v_next is unnormalized eigenvector estimate for step n, component i
    """
    if np.linalg.norm(v) == 0:
        v_next = v * 0
        u_next = u.copy()
    else:
        v_next = (1 - alpha) * v + alpha * u * np.dot(u, v) / np.linalg.norm(v)
        u_next = u - np.dot(u, v) * v / (np.linalg.norm(v) ** 2)
    return u_next, v_next


def compute_unit_vector_angular_distance(df: pd.DataFrame) -> pd.Series:
    """
    Calculate the angular distance between unit vectors.

    Accepts a df of unit vectors (each row a unit vector) and returns a series
    of consecutive angular distances indexed according to the later time point.

    The angular distance lies in [0, 1].
    """
    vecs = df.values
    # If all of the vectors are unit vectors, then
    # np.diag(vecs.dot(vecs.T)) should return an array of all 1.'s.
    cos_sim = np.diag(vecs[:-1, :].dot(vecs[1:, :].T))
    ang_dist = np.arccos(cos_sim) / np.pi
    srs = pd.Series(index=df.index[1:], data=ang_dist, name="angular change")
    return srs


def compute_eigenvector_diffs(eigenvecs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Take a list of eigenvectors and return a df of angular distances.
    """
    ang_chg = []
    for i, vec in enumerate(eigenvecs):
        srs = compute_unit_vector_angular_distance(vec)
        srs.name = i
        ang_chg.append(srs)
    df = pd.concat(ang_chg, axis=1)
    return df


# #############################################################################
# Trend + Residual decomposition
# #############################################################################


def get_trend_residual_decomp(
    signal: pd.Series,
    tau: float,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    nan_mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    Decompose a signal into trend + residual.

    - The `trend` warm-up period is set by `min_periods`
    - If `min_periods` is positive, then leading values of `trend` are NaN
      - If `nan_mode = "propagate"`, then `residual` and `trend` are Nan
        whenever at least one is
      - If `nan_mode = "restore_to_residual", then `residual` is always non-NaN
        whenever `residual` is
        - E.g., so, in particular, during any `trend` warm-up period,
          `signal = residual` and `signal = trend + residual` always holds
        - However, when the warm-up phase ends, `residual` may experience a
          large jump

    :return: dataframe with columns "trend" and "residual", indexed like
        "signal"
    """
    if nan_mode is None:
        nan_mode = "propagate"
    signal_ma = compute_smooth_moving_average(
        signal, tau, min_periods, min_depth, max_depth
    )
    df = pd.DataFrame(index=signal.index)
    df["trend"] = signal_ma
    detrended = signal - signal_ma
    if nan_mode == "restore_to_residual":
        # Restore `signal` values if `detrended` is NaN due to detrending artifacts
        # (e.g., from setting `min_periods`).
        detrended.loc[detrended.isna()] = signal
    elif nan_mode == "propagate":
        pass
    else:
        raise ValueError(f"Unrecognized nan_mode `{nan_mode}`")
    df["residual"] = detrended
    return df


# #############################################################################
# Discrete wavelet transform
# #############################################################################


def get_swt(
    sig: Union[pd.DataFrame, pd.Series],
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
    output_mode: Optional[str] = None,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Get stationary wt details and smooths for all available scales.

    If sig.index.freq == "B", then there is the following rough correspondence
    between wavelet levels and time scales:
      weekly ~ 2-3
      monthly ~ 4-5
      quarterly ~ 6
      annual ~ 8
      business cycle ~ 11

    If sig.index.freq == "T", then the approximate scales are:
      5 min ~ 2-3
      quarter hourly ~ 4
      hourly ~ 6
      daily ~ 10-11

    :param sig: input signal
    :param wavelet: pywt wavelet name, e.g., "db8"
    :param depth: the number of decomposition steps to perform. Corresponds to
        "level" parameter in `pywt.swt`
    :param timing_mode: supported timing modes are
        - "knowledge_time":
            - reindex transform according to knowledge times
            - remove warm-up artifacts
        - "zero_phase":
            - no reindexing (e.g., no phase lag in output, but transform
              timestamps are not necessarily knowledge times)
            - remove warm-up artifacts
        - "raw": `pywt.swt` as-is
    :param output_mode: valid output modes are
        - "tuple": return (smooth_df, detail_df)
        - "smooth": return smooth_df
        - "detail": return detail_df
    :return: see `output_mode`
    """
    # Choice of wavelet may significantly impact results.
    wavelet = wavelet or "haar"
    _LOG.debug("wavelet=`%s`", wavelet)
    if isinstance(sig, pd.DataFrame):
        hdbg.dassert_eq(
            sig.shape[1], 1, "Input dataframe must have a single column."
        )
        sig = sig.squeeze()
    if timing_mode is None:
        timing_mode = "knowledge_time"
    _LOG.debug("timing_mode=`%s`", timing_mode)
    if output_mode is None:
        output_mode = "tuple"
    _LOG.debug("output_mode=`%s`", output_mode)
    # Convert to numpy and pad, since the pywt swt implementation
    # requires that the input be a power of 2 in length.
    sig_len = sig.size
    padded = _pad_to_pow_of_2(sig.values)
    # Perform the wavelet decomposition.
    decomp = pywt.swt(padded, wavelet=wavelet, level=depth, norm=True)
    # Ensure we have at least one level.
    levels = len(decomp)
    _LOG.debug("levels=%d", levels)
    hdbg.dassert_lt(0, levels)
    # Reorganize wavelet coefficients. `pywt.swt` output is of the form
    #     [(cAn, cDn), ..., (cA2, cD2), (cA1, cD1)]
    smooth, detail = zip(*reversed(decomp))
    # Reorganize `swt` output into a dataframe
    # - columns indexed by `int` wavelet level (1 up to `level`)
    # - index identical to `sig.index` (padded portion deleted)
    detail_dict = {}
    smooth_dict = {}
    for level in range(1, levels + 1):
        detail_dict[level] = detail[level - 1][:sig_len]
        smooth_dict[level] = smooth[level - 1][:sig_len]
    detail_df = pd.DataFrame.from_dict(data=detail_dict)
    detail_df.index = sig.index
    smooth_df = pd.DataFrame.from_dict(data=smooth_dict)
    smooth_df.index = sig.index
    # Record wavelet width (required for removing warm-up artifacts).
    width = len(pywt.Wavelet(wavelet).filter_bank[0])
    _LOG.debug("wavelet width=%s", width)
    if timing_mode == "knowledge_time":
        for j in range(1, levels + 1):
            # Remove "warm-up" artifacts.
            _set_warmup_region_to_nan(detail_df[j], width, j)
            _set_warmup_region_to_nan(smooth_df[j], width, j)
            # Index by knowledge time.
            detail_df[j] = _reindex_by_knowledge_time(detail_df[j], width, j)
            smooth_df[j] = _reindex_by_knowledge_time(smooth_df[j], width, j)
    elif timing_mode == "zero_phase":
        for j in range(1, levels + 1):
            # Delete "warm-up" artifacts.
            _set_warmup_region_to_nan(detail_df[j], width, j)
            _set_warmup_region_to_nan(smooth_df[j], width, j)
    elif timing_mode == "raw":
        pass
    else:
        raise ValueError(f"Unsupported timing_mode `{timing_mode}`")
    # Drop columns that are all-NaNs (e.g., artifacts of padding).
    smooth_df.dropna(how="all", axis=1, inplace=True)
    detail_df.dropna(how="all", axis=1, inplace=True)
    if output_mode == "tuple":
        return smooth_df, detail_df
    if output_mode == "smooth":
        return smooth_df
    if output_mode == "detail":
        return detail_df
    raise ValueError("Unsupported output_mode `{output_mode}`")


def get_swt_level(
    sig: Union[pd.DataFrame, pd.Series],
    wavelet: str,
    level: int,
    timing_mode: Optional[str] = None,
    output_mode: Optional[str] = None,
) -> pd.Series:
    """
    Wraps `get_swt` and extracts a single wavelet level.

    :param sig: input signal
    :param wavelet: pywt wavelet name, e.g., "db8"
    :param level: the wavelet level to extract
    :param timing_mode: supported timing modes are
        - "knowledge_time":
            - reindex transform according to knowledge times
            - remove warm-up artifacts
        - "zero_phase":
            - no reindexing (e.g., no phase lag in output, but transform
              timestamps are not necessarily knowledge times)
            - remove warm-up artifacts
        - "raw": `pywt.swt` as-is
    :param output_mode: valid output modes are
        - "smooth": return smooth_df for `level`
        - "detail": return detail_df for `level`
    :return: see `output_mode`
    """
    hdbg.dassert_in(output_mode, ["smooth", "detail"])
    swt = get_swt(
        sig,
        wavelet=wavelet,
        depth=level,
        timing_mode=timing_mode,
        output_mode=output_mode,
    )
    hdbg.dassert_in(level, swt.columns)
    return swt[level]


def _pad_to_pow_of_2(arr: np.array) -> np.array:
    """
    Minimally extend `arr` with zeros so that len is a power of 2.
    """
    sig_len = arr.shape[0]
    _LOG.debug("signal length=%d", sig_len)
    pow2_ceil = int(2 ** np.ceil(np.log2(sig_len)))
    padded = np.pad(arr, (0, pow2_ceil - sig_len))
    _LOG.debug("padded length=%d", len(padded))
    return padded


def _set_warmup_region_to_nan(srs: pd.Series, width: int, level: int) -> None:
    """
    Remove warm-up artifacts by setting to `NaN`.

    NOTE: Modifies `srs` in-place.

    :srs: swt
    :width: width (length of support of mother wavelet)
    :level: wavelet level
    """
    srs[: width * 2 ** (level - 1) - width // 2] = np.nan


def _reindex_by_knowledge_time(
    srs: pd.Series, width: int, level: int
) -> pd.Series:
    """
    Shift series so that indexing is according to knowledge time.

    :srs: swt
    :width: width (length of support of mother wavelet)
    :level: wavelet level
    """
    return srs.shift(width * 2 ** (level - 1) - width // 2)


def compute_swt_var(
    sig: Union[pd.DataFrame, pd.Series],
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
    axis: int = 1,
) -> pd.DataFrame:
    """
    Get swt var using levels up to `depth`.

    Params as in `get_swt()`.
    """
    if isinstance(sig, pd.Series):
        sig = sig.to_frame()
    hdbg.dassert_eq(len(sig.columns), 1)
    col = sig.columns[0]
    df = compute_swt_covar(
        sig,
        col1=col,
        col2=col,
        wavelet=wavelet,
        depth=depth,
        timing_mode=timing_mode,
        axis=axis,
    )
    hdbg.dassert_in("swt_var", df.columns)
    return df


def compute_swt_covar(
    df: pd.DataFrame,
    col1: str,
    col2: str,
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
    axis: int = 1,
) -> pd.DataFrame:
    """
    Get swt covar using levels up to `depth`.

    Params as in `get_swt()`.
    """
    dfs = {}
    for col in [col1, col2]:
        dfs[col] = get_swt(
            df[col],
            wavelet=wavelet,
            depth=depth,
            timing_mode=timing_mode,
            output_mode="detail",
        )
    prod = dfs[col1].multiply(dfs[col2])
    fvi = prod.first_valid_index()
    col1_df = dfs[col1].loc[fvi:]
    col2_df = dfs[col2].loc[fvi:]
    if axis == 0:
        prod = prod.dropna()
        col1_df = col1_df.dropna()
        col2_df = col2_df.dropna()
    results = []
    covar_name = "swt_covar" if col1 != col2 else "swt_var"
    results.append(prod.sum(axis=axis, skipna=False).rename(covar_name))
    if col1 != col2:
        results.append(
            np.square(col1_df)
            .sum(axis=axis, skipna=False)
            .rename(str(col1) + "_swt_var")
        )
        results.append(
            np.square(col2_df)
            .sum(axis=axis, skipna=False)
            .rename(str(col2) + "_swt_var")
        )
    return pd.concat(results, axis=1)


def compute_swt_sum(
    sig: Union[pd.DataFrame, pd.Series],
    wavelet: Optional[str] = None,
    depth: Optional[int] = None,
    timing_mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get swt coeffcient sums using levels up to `depth`.

    Params as in `get_swt()`.
    """
    df = get_swt(
        sig,
        wavelet=wavelet,
        depth=depth,
        timing_mode=timing_mode,
        output_mode="detail",
    )
    srs = -1 * df.sum(axis=1, skipna=False)
    srs.name = "swt_sum"
    return srs.to_frame()


# TODO(*): Make this a decorator.
def compute_fir_zscore(
    signal: Union[pd.DataFrame, pd.Series],
    dyadic_tau: int,
    variance_dyadic_tau: Optional[int] = None,
    delay: int = 0,
    variance_delay: Optional[int] = None,
    wavelet: Optional[str] = None,
    variance_wavelet: Optional[str] = None,
) -> pd.DataFrame:
    if isinstance(signal, pd.Series):
        return _compute_fir_zscore(
            signal,
            dyadic_tau,
            variance_dyadic_tau,
            delay,
            variance_delay,
            wavelet,
            variance_wavelet,
        )
    df = signal.apply(
        lambda x: _compute_fir_zscore(
            x,
            dyadic_tau,
            variance_dyadic_tau,
            delay,
            variance_delay,
            wavelet,
            variance_wavelet,
        )
    )
    return df


def _compute_fir_zscore(
    signal: Union[pd.DataFrame, pd.Series],
    dyadic_tau: int,
    variance_dyadic_tau: Optional[int] = None,
    delay: int = 0,
    variance_delay: Optional[int] = None,
    wavelet: Optional[str] = None,
    variance_wavelet: Optional[str] = None,
) -> pd.Series:
    """
    Z-score with a FIR filter.
    """
    if variance_dyadic_tau is None:
        variance_dyadic_tau = dyadic_tau
    if variance_delay is None:
        variance_delay = delay
    if variance_wavelet is None:
        variance_wavelet = wavelet
    if isinstance(signal, pd.DataFrame):
        hdbg.dassert_eq(
            signal.shape[1], 1, "Input dataframe must have a single column."
        )
        signal = signal.squeeze()
    mean = get_swt(
        signal, wavelet=wavelet, depth=dyadic_tau, output_mode="smooth"
    )[dyadic_tau].shift(delay)
    demeaned = signal - mean
    var = get_swt(
        demeaned ** 2,
        wavelet=variance_wavelet,
        depth=variance_dyadic_tau,
        output_mode="smooth",
    )[variance_dyadic_tau].shift(variance_delay)
    # TODO(Paul): Maybe add delay-based rescaling.
    srs = demeaned / np.sqrt(var)
    srs.name = signal.name
    srs = srs.replace([-np.inf, np.inf], np.nan)
    return srs


def get_dyadic_zscored(
    sig: pd.Series, demean: bool = False, **kwargs: Any
) -> pd.DataFrame:
    """
    Z-score `sig` with successive powers of 2.

    :return: dataframe with cols named according to the exponent of 2. Number
        of cols is determined based on signal length.
    """
    pow2_ceil = int(np.ceil(np.log2(sig.size)))
    zscored = {}
    for tau_pow in range(1, pow2_ceil):
        zscored[tau_pow] = compute_rolling_zscore(
            sig, tau=2 ** tau_pow, demean=demean, **kwargs
        )
    df = pd.DataFrame.from_dict(zscored)
    return df


# #############################################################################
# Resampling
# #############################################################################


def resample(
    data: Union[pd.Series, pd.DataFrame],
    **resample_kwargs: Dict[str, Any],
) -> Union[pd.Series, pd.DataFrame]:
    """
    Execute series resampling with specified `.resample()` arguments.

    The `rule` argument must always be specified and the `closed` and `label`
    arguments are treated specially by default.

    The default values of `closed` and `label` arguments are intended to make
    pandas `resample()` behavior consistent for every value of `rule` and to
    make resampling causal. So if we have sampling times t_0 < t_1 < t_2, then,
    after resampling, the values at t_1 and t_2 should not be incorporated
    into the resampled value timestamped with t_0. Note that this behavior is
    at odds with what may be intuitive for plotting lower-frequency data, e.g.,
    yearly data is typically labeled in a plot by the start of the year.

    :data: pd.Series or pd.DataFrame with a datetime index
    :resample_kwargs: arguments for pd.DataFrame.resample
    :return: DatetimeIndexResampler object
    """
    hdbg.dassert_in("rule", resample_kwargs, "Argument 'rule' must be specified")
    # Unless specified by the user, the resampling intervals are intended as
    # (a, b] with label on the right.
    if "closed" not in resample_kwargs:
        resample_kwargs["closed"] = "right"
    if "label" not in resample_kwargs:
        resample_kwargs["label"] = "right"
    # Execute resampling with specified kwargs.
    _LOG.debug(
        "Resampling data with size=%s using kwargs='%s'",
        str(data.size),
        str(resample_kwargs),
    )
    resampled_data = data.resample(**resample_kwargs)
    _LOG.debug("resampled_data.size=%s", str(resampled_data.size))
    return resampled_data


# #############################################################################
# Special functions
# #############################################################################


def c_infinity(x: float) -> float:
    """
    Return C-infinity function evaluated at x.

    This function is zero for x <= 0 and approaches exp(1) as x ->
    infinity.
    """
    if x > 0:
        return np.exp(-1 / x)
    return 0


def c_infinity_step_function(x: float) -> float:
    """
    Return C-infinity step function evaluated at x.

    This function is
      - 0 for x <= 0
      - 1 for x >= 1
    """
    fx = c_infinity(x)
    f1mx = c_infinity(1 - x)
    if fx + f1mx == 0:
        return np.nan
    return fx / (fx + f1mx)


def c_infinity_bump_function(x: float, a: float, b: float) -> float:
    """
    Return value of C-infinity bump function evaluated at x.

    :param x: point at which to evaluate
    :param a: function is 1 between -a and a
    :param b: function is zero for abs(x) >= b
    """
    hdbg.dassert_lt(0, a)
    hdbg.dassert_lt(a, b)
    y = (x ** 2 - a ** 2) / (b ** 2 - a ** 2)
    inverse_bump = c_infinity_step_function(y)
    return 1 - inverse_bump
