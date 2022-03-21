"""
Import as:

import core.signal_processing.swt as csiprswt
"""


import logging
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd
import pywt

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


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
    # _LOG.debug("wavelet=`%s`", wavelet)
    if isinstance(sig, pd.DataFrame):
        hdbg.dassert_eq(
            sig.shape[1], 1, "Input dataframe must have a single column."
        )
        sig = sig.squeeze()
    if timing_mode is None:
        timing_mode = "knowledge_time"
    # _LOG.debug("timing_mode=`%s`", timing_mode)
    if output_mode is None:
        output_mode = "tuple"
    # _LOG.debug("output_mode=`%s`", output_mode)
    # Convert to numpy and pad, since the pywt swt implementation
    # requires that the input be a power of 2 in length.
    sig_len = sig.size
    padded = _pad_to_pow_of_2(sig.values)
    # Perform the wavelet decomposition.
    decomp = pywt.swt(padded, wavelet=wavelet, level=depth, norm=True)
    # Ensure we have at least one level.
    levels = len(decomp)
    # _LOG.debug("levels=%d", levels)
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
    # _LOG.debug("wavelet width=%s", width)
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
    if depth:
        cols = set(range(1, depth + 1))
        msg = "Insufficient data to generate transform up to requested depth."
        if not cols.issubset(smooth_df.columns):
            raise ValueError(msg)
        if not cols.issubset(detail_df.columns):
            raise ValueError(msg)
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
    arr_len = arr.shape[0]
    # _LOG.debug("array length=%d", arr_len)
    pow2_ceil = int(2 ** np.ceil(np.log2(arr_len)))
    padded = np.pad(arr, (0, pow2_ceil - arr_len))
    # _LOG.debug("padded array length=%d", len(padded))
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
    swt_var = compute_swt_var(
        sig, wavelet=wavelet, depth=depth, timing_mode=timing_mode, axis=0
    )
    hdbg.dassert_in("swt_var", swt_var.columns)
    srs = compute_swt_var(
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
    swt_covar = compute_swt_covar(
        df,
        col1,
        col2,
        wavelet=wavelet,
        depth=depth,
        timing_mode=timing_mode,
        axis=0,
    )
    hdbg.dassert_in("swt_covar", swt_covar.columns)
    srs = compute_swt_covar(
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
