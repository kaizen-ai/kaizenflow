"""
Import as:

import core.explore as expl


Utility functions for Jupyter notebook to:
- format data
- transform pandas data structures
- compute common stats

These functions are used for both interactive data exploration and to implement
more complex pipelines. The output is reported through logging.
"""

import datetime
import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
import statsmodels
import statsmodels.api
import tqdm

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)

# #############################################################################
# Helpers.
# #############################################################################


# TODO(gp): Not sure this is the right place.
def find_duplicates(vals):
    """
    Find the elements duplicated in a list.
    """
    dbg.dassert_isinstance(vals, list)
    # Count the occurrences of each element of the seq.
    # TODO(gp): Consider replacing with pd.Series.value_counts.
    v_to_num = [(v, vals.count(v)) for v in set(vals)]
    # Build list of elems with duplicates.
    res = [v for v, n in v_to_num if n > 1]
    return res


def cast_to_df(obj):
    if isinstance(obj, pd.Series):
        df = pd.DataFrame(obj)
    else:
        df = obj
    dbg.dassert_isinstance(df, pd.DataFrame)
    return df


def cast_to_series(obj):
    if isinstance(obj, pd.DataFrame):
        dbg.dassert_eq(obj.shape[1], 1)
        srs = obj.iloc[:, 1]
    else:
        srs = obj
    dbg.dassert_isinstance(srs, pd.Series)
    return srs


# #############################################################################
# Pandas helpers.
# #############################################################################


def drop_axis_with_all_nans(
    df, drop_rows=True, drop_columns=False, drop_infs=False, report_stats=False
):
    """
    Remove columns and rows completely empty.
    The operation is not in place and the resulting df is returned.

    Assume that the index is timestamps.

    :param drop_rows: remove rows with only nans
    :param drop_columns: remove columns with only nans
    :param drop_infs: remove also +/- np.inf
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    if drop_infs:
        df = df.replace([np.inf, -np.inf], np.nan)
    if drop_columns:
        # Remove columns with all nans, if any.
        cols_before = df.columns[:]
        df = df.dropna(axis=1, how="all")
        if report_stats:
            # Report results.
            cols_after = df.columns[:]
            removed_cols = set(cols_before).difference(set(cols_after))
            pct_removed = pri.perc(
                len(cols_before) - len(cols_after), len(cols_after)
            )
            _LOG.info(
                "removed cols with all nans: %s %s",
                pct_removed,
                pri.list_to_str(removed_cols),
            )
    if drop_rows:
        # Remove rows with all nans, if any.
        rows_before = df.index[:]
        df = df.dropna(axis=0, how="all")
        if report_stats:
            # Report results.
            rows_after = df.index[:]
            removed_rows = set(rows_before).difference(set(rows_after))
            if len(rows_before) == len(rows_after):
                # Nothing was removed.
                min_ts = max_ts = None
            else:
                # TODO(gp): Report as intervals of dates.
                min_ts = min(removed_rows)
                max_ts = max(removed_rows)
            pct_removed = pri.perc(
                len(rows_before) - len(rows_after), len(rows_after)
            )
            _LOG.info(
                "removed rows with all nans: %s [%s, %s]",
                pct_removed,
                min_ts,
                max_ts,
            )
    return df


def drop_na(df, drop_infs=False, report_stats=False, *args, **kwargs):
    """
    Wrapper around pd.dropna() reporting information about the removed rows.
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    num_rows_before = df.shape[0]
    if drop_infs:
        df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(*args, **kwargs)
    if report_stats:
        num_rows_after = df.shape[0]
        pct_removed = pri.perc(num_rows_before - num_rows_after, num_rows_before)
        _LOG.info("removed rows with nans: %s", pct_removed)
    return df


def report_zero_nan_inf_stats(
    df, zero_threshold=1e-9, verbose=False, as_txt=False
):
    """
    Report count and percentage about zeros, nans, infs for a df.

    :param verbose: print more information
    """
    df = cast_to_df(df)
    _LOG.info("index in [%s, %s]", df.index.min(), df.index.max())
    #
    num_rows = df.shape[0]
    _LOG.info("num_rows=%s", pri.thousand_separator(num_rows))
    _LOG.info("data=")
    display_df(df, max_lines=5, as_txt=as_txt)
    #
    num_days = len(set(df.index.date))
    _LOG.info("num_days=%s", num_days)
    #
    num_weekdays = len(set(d for d in df.index.date if d.weekday() < 5))
    _LOG.info("num_weekdays=%s", num_weekdays)
    #
    stats_df = pd.DataFrame(None, index=df.columns)
    if False:
        # Find the index of the first non-nan value.
        df = df.applymap(lambda x: not np.isnan(x))
        min_idx = df.idxmax(axis=0)
        min_idx.name = "min_idx"
        # Find the index of the last non-nan value.
        max_idx = df.reindex(index=df.index[::-1]).idxmax(axis=0)
        max_idx.name = "max_idx"
    stats_df["num_rows"] = num_rows
    #
    num_zeros = (np.abs(df) < zero_threshold).sum(axis=0)
    if verbose:
        stats_df["num_zeros"] = num_zeros
    stats_df["zeros [%]"] = (100.0 * num_zeros / num_rows).apply(pri.round_digits)
    #
    num_nans = np.isnan(df).sum(axis=0)
    if verbose:
        stats_df["num_nans"] = num_nans
    stats_df["nans [%]"] = (100.0 * num_nans / num_rows).apply(pri.round_digits)
    #
    num_infs = np.isinf(df).sum(axis=0)
    if verbose:
        stats_df["num_infs"] = num_infs
    stats_df["infs [%]"] = (100.0 * num_infs / num_rows).apply(pri.round_digits)
    #
    num_valid = df.shape[0] - num_zeros - num_nans - num_infs
    if verbose:
        stats_df["num_valid"] = num_valid
    stats_df["valid [%]"] = (100.0 * num_valid / num_rows).apply(pri.round_digits)
    #
    display_df(stats_df, as_txt=as_txt)


# //////////////////////////////////////////////////////////////////////////////


def _get_variable_cols(df, threshold=1):
    """
    Return columns of a df that contain less than <threshold> unique values.

    :return: (variable columns, constant columns)
    """
    var_cols = []
    const_cols = []
    for i in df.columns:
        if len(df[i].unique()) <= threshold:
            const_cols.append(i)
        else:
            var_cols.append(i)
    return var_cols, const_cols


def remove_columns_with_low_variability(df, threshold=1, log_level=logging.DEBUG):
    """
    Remove columns of a df that contain less than <threshold> unique values.

    :return: df with only columns with sufficient variability
    """
    var_cols, const_cols = _get_variable_cols(df, threshold=threshold)
    _LOG.log(log_level, "# Constant cols")
    for c in const_cols:
        _LOG.log(
            log_level,
            "  %s: %s",
            c,
            pri.list_to_str(list(map(str, df[c].unique()))),
        )
    _LOG.log(log_level, "# Var cols")
    _LOG.log(log_level, pri.list_to_str(var_cols))
    return df[var_cols]


def add_pct(
    df, col_name, total, dst_col_name, num_digits=2, use_thousands_separator=True
):
    """
    Add to df a column "dst_col_name" storing the percentage of values in
    column "col_name" with respect to "total".
    The rest of the parameters are the same as pri.round_digits().

    :return: updated df
    """
    # Add column with percentage right after col_name.
    pos_col_name = df.columns.tolist().index(col_name)
    df.insert(pos_col_name + 1, dst_col_name, (100.0 * df[col_name]) / total)
    # Format.
    df[col_name] = [
        pri.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator
        )
        for v in df[col_name]
    ]
    df[dst_col_name] = [
        pri.round_digits(v, num_digits=num_digits, use_thousands_separator=False)
        for v in df[dst_col_name]
    ]
    return df


# #############################################################################
# Pandas data structure stats.
# #############################################################################


# TODO(gp): Explain what this is supposed to do.
def breakdown_table(
    df, col_name, num_digits=2, use_thousands_separator=True, verbosity=False
):
    if isinstance(col_name, list):
        for c in col_name:
            print(("\n" + pri.frame(c).rstrip("\n")))
            res = breakdown_table(df, c)
            print(res)
        return None
    #
    if verbosity:
        print(("# col_name=%s" % col_name))
    first_col_name = df.columns[0]
    res = df.groupby(col_name)[first_col_name].count()
    res = pd.DataFrame(res)
    res.columns = ["count"]
    res.sort_values(["count"], ascending=False, inplace=True)
    res = res.append(
        pd.DataFrame([df.shape[0]], index=["Total"], columns=["count"])
    )
    res["pct"] = (100.0 * res["count"]) / df.shape[0]
    # Format.
    res["count"] = [
        pri.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator
        )
        for v in res["count"]
    ]
    res["pct"] = [
        pri.round_digits(v, num_digits=num_digits, use_thousands_separator=False)
        for v in res["pct"]
    ]
    if verbosity:
        for k, df_tmp in df.groupby(col_name):
            print((pri.frame("%s=%s" % (col_name, k))))
            cols = [col_name, "description"]
            with pd.option_context(
                "display.max_colwidth", 100000, "display.width", 130
            ):
                print((df_tmp[cols]))
    return res


def print_column_variability(
    df, max_num_vals=3, num_digits=2, use_thousands_separator=True
):
    """
    Print statistics about the values in each column of a data frame.
    This is useful to get a sense of which columns are interesting.
    """
    print(("# df.columns=%s" % pri.list_to_str(df.columns)))
    res = []
    for c in tqdm.tqdm(df.columns):
        vals = df[c].unique()
        min_val = min(vals)
        max_val = max(vals)
        if len(vals) <= max_num_vals:
            txt = ", ".join(map(str, vals))
        else:
            txt = ", ".join(map(str, [min_val, "...", max_val]))
        row = ["%20s" % c, len(vals), txt]
        res.append(row)
    res = pd.DataFrame(res, columns=["col_name", "num", "elems"])
    res.sort_values("num", inplace=True)
    # TODO(gp): Fix this.
    # res = add_count_as_idx(res)
    res = add_pct(
        res,
        "num",
        df.shape[0],
        "[diff %]",
        num_digits=num_digits,
        use_thousands_separator=use_thousands_separator,
    )
    res.reset_index(drop=True, inplace=True)
    return res


def find_common_columns(names, dfs):
    df = []
    for i, df1 in enumerate(dfs):
        df1 = dfs[i].columns
        for j in range(i + 1, len(dfs)):
            df2 = dfs[j].columns
            common_cols = [c for c in df1 if c in df2]
            df.append(
                (
                    names[i],
                    len(df1),
                    names[j],
                    len(df2),
                    len(common_cols),
                    ", ".join(common_cols),
                )
            )
    df = pd.DataFrame(
        df,
        columns=[
            "table1",
            "num_cols1",
            "num_cols2",
            "table2",
            "num_comm_cols",
            "common_cols",
        ],
    )
    return df


# #############################################################################
# Filter.
# #############################################################################


def remove_columns(df, cols, log_level=logging.DEBUG):
    to_remove = set(cols).intersection(set(df.columns))
    _LOG.log(log_level, "to_remove=%s", pri.list_to_str(to_remove))
    df.drop(to_remove, axis=1, inplace=True)
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.log(log_level, pri.list_to_str(df.columns))
    return df


def filter_with_df(df, filter_df, log_level=logging.DEBUG):
    """
    Compute a mask for DataFrame df using common columns and values in
    "filter_df".
    """
    mask = None
    for c in filter_df:
        dbg.dassert_in(c, df.columns)
        vals = filter_df[c].unique()
        if mask is None:
            mask = df[c].isin(vals)
        else:
            mask &= df[c].isin(vals)
    _LOG.log(log_level, "after filter=%s", pri.perc(mask.sum(), len(mask)))
    return mask


def filter_around_time(
    df,
    col_name,
    timestamp,
    timedelta_before,
    timedelta_after=None,
    log_level=logging.DEBUG,
):
    dbg.dassert_in(col_name, df)
    dbg.dassert_lte(pd.Timedelta(0), timedelta_before)
    if timedelta_after is None:
        timedelta_after = timedelta_before
    dbg.dassert_lte(pd.Timedelta(0), timedelta_after)
    #
    lower_bound = timestamp - timedelta_before
    upper_bound = timestamp + timedelta_after
    mask = (df[col_name] >= lower_bound) & (df[col_name] <= upper_bound)
    #
    _LOG.log(
        log_level,
        "Filtering in [%s, %s] selected rows=%s",
        lower_bound,
        upper_bound,
        pri.perc(mask.sum(), df.shape[0]),
    )
    return df[mask]


def filter_by_val(
    df,
    col_name,
    min_val,
    max_val,
    use_thousands_separator=True,
    log_level=logging.DEBUG,
):
    """
    Filter out rows of df where df[col_name] is not in [min_val, max_val].
    """
    # TODO(gp): If column is ordered, this can be done more efficiently with
    # binary search.
    num_rows = df.shape[0]
    if min_val is not None and max_val is not None:
        dbg.dassert_lte(min_val, max_val)
    mask = None
    if min_val is not None:
        mask = min_val <= df[col_name]
    if max_val is not None:
        mask2 = df[col_name] <= max_val
        if mask is None:
            mask = mask2
        else:
            mask &= mask2
    res = df[mask]
    dbg.dassert_lt(0, res.shape[0])
    _LOG.log(
        log_level,
        "Rows kept %s, removed %s rows",
        pri.perc(
            res.shape[0],
            num_rows,
            use_thousands_separator=use_thousands_separator,
        ),
        pri.perc(
            num_rows - res.shape[0],
            num_rows,
            use_thousands_separator=use_thousands_separator,
        ),
    )
    return res


# #############################################################################
# Plotting
# #############################################################################

# TODO(gp): Use this everywhere. Use None as default value.
_FIG_SIZE = (20, 5)

# TODO(gp): Maybe move to plotting.py?


def plot_non_na_cols(df, sort=False, ascending=True, max_num=None):
    """
    Plot a diagram describing the non-nans intervals for the columns of df.

    :param df: usual df indexed with times
    :param sort: sort the columns by number of non-nans
    :param ascending:
    :param max_num: max number of columns to plot.
    """
    # Check that there are no repeated columns.
    dbg.dassert_eq(len(find_duplicates(df.columns.tolist())), 0)
    # Note that the plot assumes that the first column is at the bottom of the
    # graph.
    # Assign 1.0 to all the non-nan value.
    df = df.applymap(lambda x: np.nan if np.isnan(x) else 1.0)
    # Sort.
    if sort:
        cnt = df.sum().sort_values(ascending=not ascending)
        df = df.reindex(cnt.index.tolist(), axis=1)
    _LOG.debug("Columns=%d %s", len(df.columns), ", ".join(df.columns))
    # Limit the number of elements.
    if max_num is not None:
        _LOG.warning(
            "Plotting only %d columns instead of all %d columns",
            max_num,
            df.shape[1],
        )
        dbg.dassert_lte(1, max_num)
        if max_num > df.shape[1]:
            _LOG.warning(
                "Too many columns requested: %d > %d", max_num, df.shape[1]
            )
        df = df.iloc[:, :max_num]
    _LOG.debug("Columns=%d %s", len(df.columns), ", ".join(df.columns))
    _LOG.debug("To plot=\n%s", df.head())
    # Associate each column to a number between 1 and num_cols + 1.
    scale = pd.Series({col: idx + 1 for idx, col in enumerate(df.columns)})
    df *= scale
    num_cols = df.shape[1]
    # Heuristics to find the value of ysize.
    figsize = None
    ysize = num_cols * 0.3
    figsize = (20, ysize)
    ax = df.plot(figsize=figsize, legend=False)
    # Force all the yticks to be equal to the column names and to be visible.
    ax.set_yticks(np.arange(num_cols, 0, -1))
    ax.set_yticklabels(reversed(df.columns.tolist()))
    return ax


def _get_heatmap_mask(corr, mode):
    if mode == "heatmap_semitriangle":
        # Generate a mask for the upper triangle.
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True
    elif mode == "heatmap":
        mask = None
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    return mask


def plot_heatmap(
    corr_df,
    mode,
    annot="auto",
    figsize=None,
    title=None,
    vmin=-1.0,
    vmax=1.0,
    ax=None,
):
    """
    Plot a heatmap for a corr / cov df.
    """
    # Sanity check.
    dbg.dassert_eq(corr_df.shape[0], corr_df.shape[1])
    dbg.dassert_lte(corr_df.shape[0], 20)
    if corr_df.shape[0] < 2 or corr_df.shape[1] < 2:
        _LOG.warning(
            "Can't plot heatmap for corr_df with shape=%s", str(corr_df.shape)
        )
        return
    if np.all(np.isnan(corr_df)):
        _LOG.warning(
            "Can't plot heatmap with only nans:\n%s", corr_df.to_string()
        )
        return
    #
    if annot == "auto":
        annot = corr_df.shape[0] < 10
    # Generate a custom diverging colormap.
    cmap = _get_heatmap_colormap()
    if figsize is None:
        figsize = (8, 6)
        # figsize = (10, 10)
    if mode in ("heatmap", "heatmap_semitriangle"):
        # Set up the matplotlib figure.
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        mask = _get_heatmap_mask(corr_df, mode)
        sns.heatmap(
            corr_df,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            # Use correct aspect ratio.
            square=True,
            annot=annot,
            fmt=".2f",
            linewidths=0.5,
            cbar_kws={"shrink": 0.5},
            mask=mask,
            ax=ax,
        )
        ax.set_title(title)
    elif mode == "clustermap":
        dbg.dassert_is(ax, None)
        g = sns.clustermap(
            corr_df,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            linewidths=0.5,
            square=True,
            annot=annot,
            figsize=figsize,
        )
        g.ax_heatmap.set_title(title)
    else:
        raise RuntimeError("Invalid mode='%s'" % mode)


# TODO(gp): Add an option to mask out the correlation with low pvalues
# http://stackoverflow.com/questions/24432101/correlation-coefficients-and-p-values-for-all-pairs-of-rows-of-a-matrix
def plot_correlation_matrix(df, mode, annot=False, figsize=None, title=None):
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return None
    # Compute the correlation matrix.
    corr_df = df.corr()
    # Plot heatmap.
    plot_heatmap(
        corr_df,
        mode,
        annot=annot,
        figsize=figsize,
        title=title,
        vmin=-1.0,
        vmax=1.0,
    )
    return corr_df


def plot_dendrogram(df, figsize=None):
    # Look at:
    # ~/.conda/envs/root_longman_20150820/lib/python2.7/site-packages/seaborn/matrix.py
    # https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return
    # y = scipy.spatial.distance.pdist(df.values, 'correlation')
    y = df.corr().values
    # z = scipy.cluster.hierarchy.linkage(y, 'single')
    z = scipy.cluster.hierarchy.linkage(y, "average")
    if figsize is None:
        figsize = (16, 16)
    _ = plt.figure(figsize=figsize)
    scipy.cluster.hierarchy.dendrogram(
        z,
        labels=df.columns.tolist(),
        leaf_rotation=0,
        color_threshold=0,
        orientation="right",
    )


def display_corr_df(df):
    if df is not None:
        df_tmp = df.applymap(lambda x: "%.2f" % x)
        display_df(df_tmp)
    else:
        _LOG.warning("Can't display correlation df since it is None")


# /////////////////////////////////////////////////////////////////////////////
# PCA
# /////////////////////////////////////////////////////////////////////////////


def get_multiple_plots(num_plots, num_cols, y_scale=None, *args, **kwargs):
    """
    Create figure to accommodate `num_plots` plots, arranged in rows with
    `num_cols` columns.
    :param num_plots: number of plots
    :param num_cols: number of columns to use in the subplot
    :param y_scale: if not None
    Return a figure and an array of axes
    """
    dbg.dassert_lte(1, num_plots)
    dbg.dassert_lte(1, num_cols)
    # Heuristic to find the dimension of the fig.
    if y_scale is not None:
        dbg.dassert_lt(0, y_scale)
        ysize = (num_plots / num_cols) * y_scale
        figsize = (20, ysize)
    else:
        figsize = None
    fig, ax = plt.subplots(
        math.ceil(num_plots / num_cols),
        num_cols,
        figsize=figsize,
        *args,
        **kwargs,
    )
    return fig, ax.flatten()


def _get_heatmap_colormap():
    """
    Generate a custom diverging colormap useful for heatmaps.
    """
    cmap = sns.diverging_palette(220, 10, as_cmap=True)
    return cmap


def _get_num_pcs_to_plot(num_pcs_to_plot, max_pcs):
    """
    Get the number of principal components to plot.
    """
    if num_pcs_to_plot == -1:
        num_pcs_to_plot = max_pcs
    dbg.dassert_lte(0, num_pcs_to_plot)
    dbg.dassert_lte(num_pcs_to_plot, max_pcs)
    return num_pcs_to_plot


# TODO(gp): Add some stats about how many nans where filled.
def handle_nans(df: pd.DataFrame, nan_mode: str) -> pd.DataFrame:
    if nan_mode == "drop":
        df = df.dropna(how="any")
    elif nan_mode == "fill_with_zero":
        df = df.fillna(0.0)
    elif nan_mode == "abort":
        num_nans = np.isnan(df).sum().sum()
        if num_nans > 0:
            raise ValueError("df has %d nans\n%s" % (num_nans, df))
    else:
        raise ValueError("Invalid nan_mode='%s'" % nan_mode)
    return df


def sample_rolling_df(rolling_df, periods):
    """
    Given a rolling metric stored as multiindex (e.g., correlation computed by
    pd.ewm) sample `periods` equispaced samples.
    :return: sampled df, array of timestamps selected
    """
    timestamps = rolling_df.index.get_level_values(0)
    ts = timestamps[:: math.ceil(len(timestamps) / periods)]
    _LOG.debug("timestamps=%s", str(ts))
    # rolling_df_out = rolling_df.unstack().reindex(ts).stack(dropna=False)
    rolling_df_out = rolling_df.loc[ts]
    return rolling_df_out, ts


# TODO(gp): Maybe we should package all the PCA code into a single object.


def plot_pca_analysis(df, plot_explained_variance=False, num_pcs_to_plot=0):
    """
    Plot results of PCA analysis for data in `df`
    - eigenvalues
    - explained variance
    - eigenvectors components
    """
    from sklearn.decomposition import PCA

    # Compute PCA.
    corr = df.corr(method="pearson")
    pca = PCA()
    pca.fit(df.fillna(0.0))
    explained_variance = pd.Series(pca.explained_variance_ratio_)
    # Find indices of assets with no nans in the covariance matrix.
    num_non_nan_corr = corr.notnull().sum()
    is_valid = num_non_nan_corr == num_non_nan_corr.max()
    valid_indices = sorted(is_valid[is_valid].index.tolist())
    # Compute eigenvalues / vectors for the subset of the matrix without nans.
    # TODO(Paul): Consider replacing `eig` with `eigh` as per
    # https://stackoverflow.com/questions/45434989
    eigenval, eigenvec = np.linalg.eig(
        corr.loc[valid_indices, valid_indices].values
    )
    # Sort by decreasing eigenvalue.
    ind = eigenval.argsort()[::-1]
    selected_pcs = eigenvec[:, ind]
    pcs = pd.DataFrame(selected_pcs, index=valid_indices)
    lambdas = pd.Series(eigenval[ind])
    # Plot explained variance.
    if plot_explained_variance:
        title = "Eigenvalues and explained variance vs ordered PCs"
        explained_variance.cumsum().plot(title=title, lw=5, ylim=(0, 1))
        # Plot principal component lambda.
        (lambdas / lambdas.max()).plot(color="g", kind="bar")
    # Plot eigenvectors.
    max_pcs = len(lambdas)
    num_pcs_to_plot = _get_num_pcs_to_plot(num_pcs_to_plot, max_pcs)
    _LOG.info("num_pcs_to_plot=%s", num_pcs_to_plot)
    if num_pcs_to_plot > 0:
        _, axes = get_multiple_plots(
            num_pcs_to_plot, num_cols=4, sharex=True, sharey=True
        )
        for i in range(num_pcs_to_plot):
            pc = pcs.ix[:, i]
            pc.plot(kind="barh", ax=axes[i], ylim=(-1, 1), title="PC%s" % i)


# NOTE:
#   - DRY: We have a rolling corr function elsewhere.
#   - Functional style: This one seems to be able to modify `ret` through
#     `nan_mode`.
def rolling_corr_over_time(df, com, nan_mode):
    """
    Compute rolling correlation over time.
    :return: corr_df is a multi-index df storing correlation matrices with
        labels
    """
    dbg.dassert_monotonic_index(df)
    df = handle_nans(df, nan_mode)
    corr_df = df.ewm(com=com, min_periods=3 * com).corr()
    return corr_df


def plot_corr_over_time(corr_df, mode, annot=False, num_cols=4):
    """
    Plot correlation over time.
    """
    timestamps = corr_df.index.get_level_values(0).unique()
    dbg.dassert_lte(len(timestamps), 20)
    # Get the axes.
    fig, axes = get_multiple_plots(
        len(timestamps), num_cols=num_cols, y_scale=4, sharex=True, sharey=True
    )
    # Add color map bar on the side.
    cbar_ax = fig.add_axes([0.91, 0.3, 0.03, 0.4])
    cmap = _get_heatmap_colormap()
    for i, dt in enumerate(timestamps):
        corr_tmp = corr_df.loc[dt]
        # Generate a mask for the upper triangle.
        mask = _get_heatmap_mask(corr_tmp, mode)
        # Plot.
        sns.heatmap(
            corr_tmp,
            cmap=cmap,
            cbar=i == 0,
            cbar_ax=None if i else cbar_ax,
            vmin=-1,
            vmax=1,
            square=True,
            annot=annot,
            fmt=".2f",
            linewidths=0.5,
            mask=mask,
            # cbar_kws={"shrink": .5},
            ax=axes[i],
        )
        axes[i].set_title(timestamps[i])


def rolling_pca_over_time(df, com, nan_mode, sort_eigvals=True):
    """
    Compute rolling PCAs over time.
    :param sort_eigvals: sort the eigenvalues in descending orders
    :return:
        - eigval_df stores eigenvalues for the different components indexed by
          timestamps
        - eigvec_df stores eigenvectors as multiindex df
    """
    # Compute rolling correlation.
    corr_df = rolling_corr_over_time(df, com, nan_mode)
    # Compute eigvalues and eigenvectors.
    eigval_df = []
    eigvec_df = []
    timestamps = corr_df.index.get_level_values(0).unique()
    for dt in tqdm.tqdm(timestamps):
        dbg.dassert_isinstance(dt, datetime.date)
        corr_tmp = corr_df.loc[dt].copy()
        # Compute rolling eigenvalues and eigenvectors.
        # TODO(gp): Count and report inf and nans as warning.
        corr_tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
        corr_tmp.fillna(0.0, inplace=True)
        eigval, eigvec = np.linalg.eig(corr_tmp)
        # Sort eigenvalues, if needed.
        if not (sorted(eigval) == eigval).all():
            _LOG.debug("eigvals not sorted: %s", eigval)
            if sort_eigvals:
                _LOG.debug(
                    "Before sorting:\neigval=\n%s\neigvec=\n%s", eigval, eigvec
                )
                _LOG.debug("eigvals: %s", eigval)
                idx = eigval.argsort()[::-1]
                eigval = eigval[idx]
                eigvec = eigvec[:, idx]
                _LOG.debug(
                    "After sorting:\neigval=\n%s\neigvec=\n%s", eigval, eigvec
                )
        #
        eigval_df.append(eigval)
        #
        if (eigval == 0).all():
            eigvec = np.nan * eigvec
        eigvec_df_tmp = pd.DataFrame(eigvec, index=corr_tmp.columns)
        # Add another index.
        eigvec_df_tmp.index.name = ""
        eigvec_df_tmp.reset_index(inplace=True)
        eigvec_df_tmp.insert(0, "datetime", dt)
        eigvec_df_tmp.set_index(["datetime", ""], inplace=True)
        eigvec_df.append(eigvec_df_tmp)
    # Package results.
    eigval_df = pd.DataFrame(eigval_df, index=timestamps)
    dbg.dassert_eq(eigval_df.shape[0], len(timestamps))
    dbg.dassert_monotonic_index(eigval_df)
    # Normalize by sum.
    # TODO(gp): Move this up.
    eigval_df = eigval_df.multiply(1 / eigval_df.sum(axis=1), axis="index")
    #
    eigvec_df = pd.concat(eigvec_df, axis=0)
    dbg.dassert_eq(
        len(eigvec_df.index.get_level_values(0).unique()), len(timestamps)
    )
    return corr_df, eigval_df, eigvec_df


def plot_pca_over_time(eigval_df, eigvec_df, num_pcs_to_plot=0, num_cols=2):
    """
    Similar to plot_pca_analysis() but over time.
    """
    # Plot eigenvalues.
    eigval_df.plot(title="Eigenvalues over time", ylim=(0, 1))
    # Plot cumulative variance.
    eigval_df.cumsum(axis=1).plot(
        title="Fraction of variance explained by top PCs over time", ylim=(0, 1)
    )
    # Plot eigenvalues.
    max_pcs = eigvec_df.shape[1]
    num_pcs_to_plot = _get_num_pcs_to_plot(num_pcs_to_plot, max_pcs)
    _LOG.info("num_pcs_to_plot=%s", num_pcs_to_plot)
    if num_pcs_to_plot > 0:
        _, axes = get_multiple_plots(
            num_pcs_to_plot,
            num_cols=num_cols,
            y_scale=4,
            sharex=True,
            sharey=True,
        )
        for i in range(num_pcs_to_plot):
            eigvec_df[i].unstack(1).plot(
                ax=axes[i], ylim=(-1, 1), title="PC%s" % i
            )


def plot_time_distributions(dts, mode, density=True):
    """
    Compute distribution for an array of timestamps `dts`.
    - mode: see below
    """
    dbg.dassert_type_in(dts[0], (datetime.datetime, pd.Timestamp))
    dbg.dassert_in(
        mode,
        (
            "time_of_the_day",
            "weekday",
            "minute_of_the_hour",
            "day_of_the_month",
            "month_of_the_year",
            "year",
        ),
    )
    if mode == "time_of_the_day":
        # Converts in minutes from the beginning of the day.
        data = [dt.time() for dt in dts]
        data = [t.hour * 60 + t.minute for t in data]
        # 1 hour bucket.
        step = 60
        bins = np.arange(0, 24 * 60 + step, step)
        vals = pd.cut(
            data,
            bins=bins,
            include_lowest=True,
            right=False,
            retbins=False,
            labels=False,
        )
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = ["%02d:%02d" % (bins[k] / 60, bins[k] % 60) for k in count.index]
    elif mode == "weekday":
        data = [dt.date().weekday() for dt in dts]
        bins = np.arange(0, 7 + 1)
        vals = pd.cut(
            data,
            bins=bins,
            include_lowest=True,
            right=False,
            retbins=False,
            labels=False,
        )
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = "Mon Tue Wed Thu Fri Sat Sun".split()
    elif mode == "minute_of_the_hour":
        vals = [dt.time().minute for dt in dts]
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = list(map(str, list(range(1, 60 + 1))))
    elif mode == "day_of_the_month":
        vals = [dt.date().day for dt in dts]
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = list(map(str, list(range(1, 31 + 1))))
    elif mode == "month_of_the_year":
        vals = [dt.date().month for dt in dts]
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
    elif mode == "year":
        vals = [dt.date().year for dt in dts]
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = pd.Series(vals).unique().tolist()
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    dbg.dassert_eq(count.sum(), len(dts))
    #
    if density:
        count /= count.sum()
    label = "num points=%s" % len(dts)
    ax = count.plot(kind="bar", label=label, figsize=(20, 7))
    ax.set_xticklabels(yticks)
    if density:
        ax.set_ylabel("Probability")
    else:
        ax.set_ylabel("Count")
    ax.legend(loc="best")
    return ax


# TODO(gp): It can't accept ax. Remove this limitation.
def jointplot(
    df: pd.DataFrame,
    predicted_var: str,
    predictor_var: str,
    height: int = None,
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    Wrapper to perform a scatterplot of two columns of a dataframe using
    seaborn.jointplot().

    :param df: dataframe
    :param predicted_var: y-var
    :param predictor_var: x-var
    :param args, kwargs: arguments passed to seaborn.jointplot()
    """
    dbg.dassert_in(predicted_var, df.columns)
    dbg.dassert_in(predictor_var, df.columns)
    df = df[[predicted_var, predictor_var]]
    # Remove non-finite values.
    # TODO(gp): Use explore.dropna().
    mask = np.all(np.isfinite(df.values), axis=1)
    df = df[mask]
    # Plot.
    sns.jointplot(
        predictor_var, predicted_var, df, height=height, *args, **kwargs
    )


def _preprocess_regression(
    df: pd.DataFrame,
    intercept: bool,
    predicted_var: str,
    predicted_var_delay: int,
    predictor_vars: str,
    predictor_vars_delay: int,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Preprocess data in dataframe form in order to perform a regression.
    """
    # Sanity check vars.
    dbg.dassert_type_is(df, pd.DataFrame)
    dbg.dassert_lte(1, df.shape[0])
    if isinstance(predictor_vars, str):
        predictor_vars = [predictor_vars]
    dbg.dassert_type_is(predictor_vars, list)
    # dbg.dassert_type_is(predicted_var, str)
    dbg.dassert_not_in(predicted_var, predictor_vars)
    if not predictor_vars:
        # No predictors.
        _LOG.warning("No predictor vars: skipping")
        return None
    #
    col_names = [predicted_var] + predictor_vars
    dbg.dassert_is_subset(col_names, df.columns)
    df = df[col_names].copy()
    num_rows = df.shape[0]
    # Shift.
    if predicted_var_delay != 0:
        df[predicted_var] = df[predicted_var].shift(predicted_var_delay)
        _LOG.warning("Shifting predicted_var=%s", predicted_var_delay)
    if predictor_vars_delay != 0:
        df[predictor_vars] = df[predictor_vars].shift(predictor_vars_delay)
        _LOG.warning("Shifting predictor_vars=%s", predictor_vars_delay)
    # Remove non-finite values.
    # TODO(gp): Use the function.
    df.dropna(how="all", inplace=True)
    num_rows_after_drop_nan_all = df.shape[0]
    if num_rows_after_drop_nan_all != num_rows:
        _LOG.info(
            "Removed %s rows with all nans",
            pri.perc(num_rows - num_rows_after_drop_nan_all, num_rows),
        )
    #
    df.dropna(how="any", inplace=True)
    num_rows_after_drop_nan_any = df.shape[0]
    if num_rows_after_drop_nan_any != num_rows_after_drop_nan_all:
        _LOG.warning(
            "Removed %s rows with any nans",
            pri.perc(num_rows - num_rows_after_drop_nan_any, num_rows),
        )
    # Prepare data.
    if intercept:
        if "const" not in df.columns:
            df.insert(0, "const", 1.0)
        predictor_vars = ["const"] + predictor_vars[:]
    param_names = predictor_vars[:]
    dbg.dassert(np.all(np.isfinite(df[predicted_var].values)))
    dbg.dassert(
        np.all(np.isfinite(df[predictor_vars].values)),
        msg="predictor_vars=%s" % predictor_vars,
    )
    # Perform regression.
    if df.shape[0] < 1:
        return None
    return df, param_names, predictor_vars


def ols_regress(
    df: pd.DataFrame,
    predicted_var: str,
    predictor_vars: str,
    intercept: bool,
    print_model_stats: bool = True,
    tsplot: bool = False,
    tsplot_figsize: Optional[Any] = None,
    jointplot_: bool = True,
    jointplot_height: Optional[Any] = None,
    predicted_var_delay: int = 0,
    predictor_vars_delay: int = 0,
    max_nrows: float = 1e4,
) -> Dict[str, Any]:
    """
    Perform OLS on columns of a dataframe.

    :param df: dataframe
    :param predicted_var: y variable
    :param predictor_vars: x variables
    :param intercept:
    :param print_model_stats: print or return the model stats
    :param tsplot: plot a time-series if possible
    :param tsplot_figsize:
    :param jointplot_: plot a scatter plot
    :param jointplot_height:
    :param predicted_var_delay:
    :param predictor_vars_delay:
    :param max_nrows: do not plot if there are too many rows, since notebook
        can be slow or hang
    :return:
    """
    obj = _preprocess_regression(
        df,
        intercept,
        predicted_var,
        predicted_var_delay,
        predictor_vars,
        predictor_vars_delay,
    )
    if obj is None:
        return None
    df, param_names, predictor_vars = obj
    dbg.dassert_lte(1, df.shape[0])
    model = statsmodels.api.OLS(
        df[predicted_var], df[predictor_vars], hasconst=intercept
    ).fit()
    regr_res = {
        "param_names": param_names,
        "coeffs": model.params,
        "pvals": model.pvalues,
        # pylint: disable=no-member
        "rsquared": model.rsquared,
        "adj_rsquared": model.rsquared_adj,
        "model": model,
    }
    if print_model_stats:
        # pylint: disable=no-member
        _LOG.info(model.summary().as_text())
    if tsplot or jointplot_:
        if max_nrows is not None and df.shape[0] > max_nrows:
            _LOG.warning(
                "Skipping plots since df has %d > %d rows", df.shape[0], max_nrows
            )
        else:
            predictor_vars = [p for p in predictor_vars if p != "const"]
            if len(predictor_vars) == 1:
                if tsplot:
                    # Plot the data over time.
                    if tsplot_figsize is None:
                        tsplot_figsize = _FIG_SIZE
                    df[[predicted_var, predictor_vars[0]]].plot(
                        figsize=tsplot_figsize
                    )
                if jointplot_:
                    # Perform scatter plot.
                    if jointplot_height is None:
                        jointplot_height = _FIG_SIZE[1]
                    jointplot(
                        df,
                        predicted_var,
                        predictor_vars[0],
                        height=jointplot_height,
                    )
            else:
                _LOG.warning("Skipping plots since there are too many predictors")
    if print_model_stats:
        return None
    return regr_res


def to_series(obj: Any) -> pd.Series:
    if isinstance(obj, np.ndarray):
        dbg.dassert(obj.shape, 1)
        srs = pd.Series(obj)
    else:
        srs = obj
    dbg.dassert_isinstance(srs, pd.Series)
    return srs


def ols_regress_series(
    srs1: pd.Series,
    srs2: pd.Series,
    intercept: bool,
    srs1_name: Optional[Any] = None,
    srs2_name: Optional[Any] = None,
    convert_to_dates: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Wrapper around regress() to regress series against each other.
    """
    srs1 = to_series(srs1).copy()
    srs2 = to_series(srs2).copy()
    #
    if convert_to_dates:
        _LOG.warning("Sampling to date")
        srs1.index = [pd.to_datetime(dt).date() for dt in srs1.index]
        srs2.index = [pd.to_datetime(dt).date() for dt in srs2.index]
    #
    dbg.dassert_array_has_same_type_element(srs1, srs2, only_first_elem=True)
    # Check common indices.
    common_idx = srs1.index.intersection(srs2.index)
    dbg.dassert_lte(1, len(common_idx))
    # Merge series into a dataframe.
    if srs1_name is None:
        srs1_name = srs1.name if srs1.name is not None else ""
    if srs2_name is None:
        srs2_name = srs2.name if srs2.name is not None else ""
    if srs1_name == srs2_name:
        srs1_name += "_1"
        srs2_name += "_2"
        _LOG.warning("Series have the same name: adding suffix to distinguish")
    df = pd.concat([srs1, srs2], axis=1, join="outer")
    df.columns = [srs1_name, srs2_name]
    #
    val = ols_regress(df, srs1_name, srs2_name, intercept=intercept, **kwargs)
    return val


def pvalue_to_stars(pval):
    if np.isnan(pval):
        stars = "NA"
    else:
        dbg.dassert_lte(0.0, pval)
        dbg.dassert_lte(pval, 1.0)
        if pval < 0.005:
            # More than 99.5% confidence.
            stars = "****"
        elif pval < 0.01:
            # More than 99% confidence.
            stars = "***"
        elif pval < 0.05:
            # More than 95% confidence.
            stars = "**"
        elif pval < 0.1:
            # More than 90% confidence.
            stars = "*"
        else:
            stars = "?"
    return stars


def format_ols_regress_results(regr_res):
    if regr_res is None:
        _LOG.warning("regr_res=None: skipping")
        df = pd.DataFrame(None)
        return df
    row = [
        "%.3f (%s)" % (coeff, pvalue_to_stars(pval))
        for (coeff, pval) in zip(regr_res["coeffs"], regr_res["pvals"])
    ]
    row.append(float("%.2f" % (regr_res["rsquared"] * 100.0)))
    row.append(float("%.2f" % (regr_res["adj_rsquared"] * 100.0)))
    col_names = regr_res["param_names"] + ["R^2 [%]", "Adj R^2 [%]"]
    df = pd.DataFrame([row], columns=col_names)
    return df


def robust_regression(
    df,
    predicted_var,
    predictor_vars,
    intercept,
    jointplot_=True,
    jointplot_figsize=None,
    predicted_var_delay=0,
    predictor_vars_delay=0,
):
    obj = _preprocess_regression(
        df,
        intercept,
        predicted_var,
        predicted_var_delay,
        predictor_vars,
        predictor_vars_delay,
    )
    if obj is None:
        return
    # From http://scikit-learn.org/stable/auto_examples/linear_model/
    #   plot_robust_fit.html#sphx-glr-auto-examples-linear-model-plot-robust-fit-py
    # TODO(gp): Add also TheilSenRegressor and HuberRegressor.
    from sklearn import linear_model

    dbg.dassert_eq(len(predictor_vars), 1)
    y = df[predicted_var]
    X = df[predictor_vars]
    # Fit line using all data.
    lr = linear_model.LinearRegression()
    lr.fit(X, y)
    # Robustly fit linear model with RANSAC algorithm.
    ransac = linear_model.RANSACRegressor()
    ransac.fit(X, y)
    inlier_mask = ransac.inlier_mask_
    outlier_mask = np.logical_not(inlier_mask)
    # Predict data of estimated models.
    line_X = np.linspace(X.min().values[0], X.max().values[0], num=100)[
        :, np.newaxis
    ]
    line_y = lr.predict(line_X)
    line_y_ransac = ransac.predict(line_X)
    # Compare estimated coefficients
    _LOG.info("Estimated coef for linear regression=%s", lr.coef_)
    _LOG.info("Estimated coef for RANSAC=%s", ransac.estimator_.coef_)
    if jointplot_:
        if jointplot_figsize is None:
            jointplot_figsize = _FIG_SIZE
        plt.figure(figsize=jointplot_figsize)
        plt.scatter(
            X[inlier_mask],
            y[inlier_mask],
            color="red",
            marker="o",
            label="Inliers",
        )
        plt.scatter(
            X[outlier_mask],
            y[outlier_mask],
            color="blue",
            marker="o",
            label="Outliers",
        )
        plt.plot(line_X, line_y, color="green", linewidth=2, label="OLS")
        plt.plot(
            line_X, line_y_ransac, color="black", linewidth=3, label="RANSAC"
        )
        plt.legend(loc="best")
        plt.xlabel(", ".join(predictor_vars))
        plt.ylabel(predicted_var)


# #############################################################################
# Statistics.
# #############################################################################


def adf(srs, verbose=False):
    """
    Wrapper around statsmodels.adfuller().

    :param verbose: return all info, instead of just p-value.
    :return: srs
    """
    # https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.adfuller.html
    srs = cast_to_series(srs)
    from statsmodels.tsa.stattools import adfuller

    adf_stat, pvalue, usedlag, nobs, critical_values, icbest = adfuller(
        srs.values
    )
    # E.g.,
    # (-25.618120847156426, 0.0, 1, 998,
    # {'1%': -3.4369193380671,
    #   '5%': -2.864440383452517,
    #   '10%': -2.56831430323573},
    # 2658.933246559476)
    res = [("pvalue", pvalue)]
    if verbose:
        res.extend(
            [
                ("adf_stat", adf_stat),
                ("usedlag", usedlag),
                ("nobs", nobs),
                ("critical_values_1%", critical_values["1%"]),
                ("critical_values_5%", critical_values["5%"]),
                ("critical_values_10%", critical_values["10%"]),
                ("icbest", icbest),
            ]
        )
    data = list(zip(*res))
    res = pd.Series(data[1], index=data[0])
    return res


# #############################################################################
# Printing
# #############################################################################


def display_df(
    df,
    index=True,
    inline_index=False,
    max_lines=5,
    as_txt=False,
    tag=None,
    mode=None,
):
    """
    Display a pandas object (series, df, panel) in a better way than the
    ipython display, e.g.,
        - by printing head and tail of the dataframe
        - by formatting the code

    :param index: whether to show the index or not
    :param inline_index: make the index part of the dataframe. This is used
        when cutting and pasting to other applications, which are not happy
        with the output pandas html form
    :param max_lines: number of lines to print
    :param as_txt: print if True, otherwise render as usual html
    :param mode: use different formats temporarily overriding the default, e.g.,
        - "all_rows": print all the rows
        - "all_cols": print all the columns
        - "all": print the entire df (it could be huge)
    """
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)
    elif isinstance(df, pd.Panel):
        for c in list(df.keys()):
            print("# %s" % c)
            df_tmp = df[c]
            display_df(
                df_tmp,
                index=index,
                inline_index=inline_index,
                max_lines=max_lines,
                as_txt=as_txt,
                mode=mode,
            )
        return
    #
    dbg.dassert_type_is(df, pd.DataFrame)
    dbg.dassert_eq(
        find_duplicates(df.columns.tolist()), [], msg="Find duplicated columns"
    )
    if tag is not None:
        print(tag)
    if max_lines is not None:
        dbg.dassert_lte(1, max_lines)
        if df.shape[0] > max_lines:
            # log.error("Printing only top / bottom %s out of %s rows",
            #        max_lines, df.shape[0])
            ellipses = pd.DataFrame(
                [["..."] * len(df.columns)], columns=df.columns, index=["..."]
            )
            df = pd.concat(
                [
                    df.head(int(max_lines / 2)),
                    ellipses,
                    df.tail(int(max_lines / 2)),
                ],
                axis=0,
            )
    if inline_index:
        df = df.copy()
        # Copy the index to a column and don't print the index.
        if df.index.name is None:
            col_name = "."
        else:
            col_name = df.index.name
        df.insert(0, col_name, df.index)
        df.index.name = None
        index = False

    # Finally, print / display.
    def _print_display():
        if as_txt:
            print(df.to_string(index=index))
        else:
            import IPython.core.display

            IPython.core.display.display(
                IPython.core.display.HTML(df.to_html(index=index))
            )

    if mode is None:
        _print_display()
    elif mode == "all_rows":
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", 3
        ):
            _print_display()
    elif mode == "all_cols":
        with pd.option_context(
            "display.max_colwidth", int(1e6), "display.max_columns", None
        ):
            _print_display()
    elif mode == "all":
        with pd.option_context(
            "display.max_rows",
            int(1e6),
            "display.max_columns",
            3,
            "display.max_colwidth",
            int(1e6),
            "display.max_columns",
            None,
        ):
            _print_display()
    else:
        _print_display()
        raise ValueError("Invalid mode=%s" % mode)


def describe_df(
    df,
    ts_col=None,
    max_col_width=30,
    max_thr=15,
    sort_by_uniq_num=False,
    log_level=logging.INFO,
):
    """
    Improved version of pd.DataFrame.describe()
    :param ts_col: timestamp column
    """
    if ts_col is None:
        _LOG.log(
            log_level,
            "[%s, %s], count=%s",
            min(df.index),
            max(df.index),
            len(df.index.unique()),
        )
    else:
        _LOG.log(
            log_level,
            "%s: [%s, %s], count=%s",
            ts_col,
            min(df[ts_col]),
            max(df[ts_col]),
            len(df[ts_col].unique()),
        )
    _LOG.log(log_level, "num_cols=%s", df.shape[1])
    _LOG.log(log_level, "num_rows=%s", df.shape[0])
    res_df = []
    for c in df.columns:
        uniq = df[c].unique()
        num = len(uniq)
        if num < max_thr:
            vals = " ".join(map(str, uniq))
        else:
            vals = " ".join(map(str, uniq[:10]))
        if len(vals) > max_col_width:
            vals = vals[:max_col_width] + " ..."
        type_str = df[c].dtype
        res_df.append([c, len(uniq), vals, type_str])
    #
    res_df = pd.DataFrame(res_df, columns=["column", "num uniq", "vals", "type"])
    res_df.set_index("column", inplace=True)
    if sort_by_uniq_num:
        res_df.sort("num uniq", inplace=True)
    _LOG.log(log_level, "res_df=\n%s", res_df)
