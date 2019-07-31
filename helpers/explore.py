"""
Utility functions for Jupyter notebook for:
- formatting
- transforming pandas data structures
- computing common stats

These functions are used for both interactive data exploration and to implement
more complex pipelines. The output is reported through logging.
"""

import collections
import datetime
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
import statsmodels.api as sm
from tqdm import tqdm

import helpers.dbg as dbg
import helpers.printing as printing

_LOG = logging.getLogger(__name__)

# #############################################################################
# Pandas helpers.
# #############################################################################


def find_duplicates(vals):
    # Count the occurrences of each element of the seq.
    v_to_num = [(v, vals.count(v)) for v in set(vals)]
    # Build list of elems with duplicates.
    res = [v for v, n in v_to_num if n > 1]
    return res


# #############################################################################
# Pandas helpers.
# #############################################################################


def drop_na_rows_columns(df):
    """
    Remove columns and rows completely empty.
    Assume that the index is timestamps, while columns are features.

    :param df:
    :return:
    """
    # Remove columns with all nans, if any.
    cols_before = df.columns[:]
    df = df.dropna(axis=1, how="all")
    cols_after = df.columns[:]
    # Report results.
    removed_cols = [
        x in cols_after for x in set(cols_before).difference(set(cols_after))
    ]
    pct_removed = printing.perc(
        len(cols_before) - len(cols_after), len(cols_after))
    _LOG.info("removed cols with all nans: %s %s", pct_removed,
              printing.list_to_str(removed_cols))
    #
    # Remove rows with all nans, if any.
    rows_before = df.columns[:]
    df = df.dropna(axis=0, how="all")
    rows_after = df.columns[:]
    # Report results.
    removed_rows = [
        x in rows_after for x in set(rows_before).difference(set(rows_after))
    ]
    if len(rows_before) == len(rows_after):
        # Nothing was removed.
        min_ts = max_ts = None
    else:
        # TODO(gp): Report as intervals of dates.
        min_ts = min(removed_rows)
        max_ts = max(removed_rows)
    pct_removed = printing.perc(
        len(rows_before) - len(rows_after), len(rows_after))
    _LOG.info("removed rows with all nans: %s [%s, %s]", pct_removed, min_ts,
              max_ts)
    return df


def drop_na(df, *args, **kwargs):
    """
    Wrapper around pd.dropna() reporting information about the removed rows.

    :param df:
    :param args:
    :param kwargs:
    :return:
    """
    # TODO(gp): Remove rows completely empty.
    num_rows_before = df.shape[0]
    df = df.dropna(*args, **kwargs)
    log_level = logging.INFO
    if "log_level" in kwargs:
        log_level = kwargs["log_level"]
    num_rows_after = df.shape[0]
    pct_removed = printing.perc(num_rows_before - num_rows_after,
                                num_rows_before)
    _LOG.log(log_level, "removed rows with nans: %s", pct_removed)
    return df


# //////////////////////////////////////////////////////////////////////////////


def _get_variable_cols(df, threshold=1):
    """
    Return columns of a df that contain less than <threshold> unique values.

    :return: (variable cols, const_cols)
    """
    var_cols = []
    const_cols = []
    for i in df.columns:
        if len(df[i].unique()) <= threshold:
            const_cols.append(i)
        else:
            var_cols.append(i)
    return var_cols, const_cols


# TODO(gp): Remove verb and use print.
def remove_columns_with_low_variability(df,
                                        threshold=1,
                                        log_level=logging.DEBUG):
    """
    Remove columns of df that contain less than <threshold> unique values.

    :return: df with only variable columns
    """
    var_cols, const_cols = _get_variable_cols(df, threshold=threshold)
    _LOG.log(log_level, "# Constant cols")
    for c in const_cols:
        _LOG.log(log_level, "  %s: %s", c,
                 printing.list_to_str(list(map(str, df[c].unique()))))
    _LOG.log(log_level, "# Var cols")
    _LOG.log(log_level, printing.list_to_str(var_cols))
    return df[var_cols]


# TODO(gp): ?
def add_count_as_idx(df):
    col = []
    for k in range(df.shape[0]):
        col.append("%2.d / %2.d" % (k + 1, df.shape[0]))
    df.insert(0, "index", col)
    return df


# TODO(gp): ?
def add_pct(res,
            col_name,
            total,
            dst_col_name,
            num_digits=2,
            use_thousands_separator=True):
    # Add column with percentage right after col_name.
    pos_col_name = res.columns.tolist().index(col_name)
    res.insert(pos_col_name + 1, dst_col_name, (100.0 * res[col_name]) / total)
    # Format.
    res[col_name] = [
        printing.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator)
        for v in res[col_name]
    ]
    res[dst_col_name] = [
        printing.round_digits(
            v, num_digits=num_digits, use_thousands_separator=False)
        for v in res[dst_col_name]
    ]
    return res


# #############################################################################
# Pandas data structure stats.
# #############################################################################


def describe_nan_stats(df):
    """
    Find the first and the last non-nan values for each column.
    """
    # Find the index of the first non-nan value.
    df = df.applymap(lambda x: not np.isnan(x))
    min_idx = df.idxmax(axis=0)
    min_idx.name = "min_idx"
    # Find the index of the last non-nan value.
    max_idx = df.reindex(index=df.index[::-1]).idxmax(axis=0)
    max_idx.name = "max_idx"
    count_idx = df.sum(axis=0)
    count_idx.name = "num_non_nans"
    pct_idx = count_idx / df.shape[0]
    pct_idx.name = "pct_non_nans"
    # Package result into a df with a row for each statistic.
    ret_df = pd.concat([
        pd.DataFrame(df_tmp).T
        for df_tmp in [min_idx, max_idx, count_idx, pct_idx]
    ])
    return ret_df


def breakdown_table(df,
                    col_name,
                    num_digits=2,
                    use_thousands_separator=True,
                    verb=False):
    if isinstance(col_name, list):
        for c in col_name:
            print(("\n" + printing.frame(c).rstrip("\n")))
            res = breakdown_table(df, c)
            print(res)
        return None
    #
    if verb:
        print(("# col_name=%s" % col_name))
    first_col_name = df.columns[0]
    res = df.groupby(col_name)[first_col_name].count()
    res = pd.DataFrame(res)
    res.columns = ["count"]
    res.sort_values(["count"], ascending=False, inplace=True)
    res = res.append(
        pd.DataFrame([df.shape[0]], index=["Total"], columns=["count"]))
    res["pct"] = (100.0 * res["count"]) / df.shape[0]
    # Format.
    res["count"] = [
        printing.round_digits(
            v, num_digits=None, use_thousands_separator=use_thousands_separator)
        for v in res["count"]
    ]
    res["pct"] = [
        printing.round_digits(
            v, num_digits=num_digits, use_thousands_separator=False)
        for v in res["pct"]
    ]
    if verb:
        for k, df_tmp in df.groupby(col_name):
            print((printing.frame("%s=%s" % (col_name, k))))
            cols = [col_name, "description"]
            with pd.option_context("display.max_colwidth", 100000,
                                   "display.width", 130):
                print((df_tmp[cols]))
    return res


def printingcolumn_variability(df,
                               max_num_vals=3,
                               num_digits=2,
                               use_thousands_separator=True):
    """
    Print statistics about the values in each column of a data frame.
    This is useful to get a sense of which columns are interesting.
    """
    print(("# df.columns=%s" % printing.list_to_str(df.columns)))
    res = []
    for c in tqdm(df.columns):
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
    res = add_count_as_idx(res)
    res = add_pct(
        res,
        "num",
        df.shape[0],
        "[diff %]",
        num_digits=num_digits,
        use_thousands_separator=use_thousands_separator)
    res.reset_index(drop=True, inplace=True)
    return res


def find_common_columns(names, dfs):
    df = []
    for i, df1 in enumerate(dfs):
        df1 = dfs[i].columns
        for j in range(i + 1, len(dfs)):
            df2 = dfs[j].columns
            common_cols = [c for c in df1 if c in df2]
            df.append((names[i], len(df1), names[j], len(df2), len(common_cols),
                       ", ".join(common_cols)))
    df = pd.DataFrame(
        df,
        columns=[
            "table1", "num_cols1", "num_cols2", "table2", "num_comm_cols",
            "common_cols"
        ])
    return df


# #############################################################################
# Filter.
# #############################################################################


def remove_columns(df, cols, log_level=logging.DEBUG):
    to_remove = set(cols).intersection(set(df.columns))
    _LOG.log(log_level, "to_remove=%s", printing.list_to_str(to_remove))
    df.drop(to_remove, axis=1, inplace=True)
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.log(log_level, printing.list_to_str(df.columns))
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
    _LOG.log(log_level, "after filter=%s", printing.perc(mask.sum(), len(mask)))
    return mask


def filter_around_time(df,
                       col_name,
                       timestamp,
                       timedelta_before,
                       timedelta_after=None,
                       log_level=logging.DEBUG):
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
    _LOG.log(log_level, "Filtering in [%s, %s] selected rows=%s", lower_bound,
             upper_bound, printing.perc(mask.sum(), df.shape[0]))
    return df[mask]


def filter_by_val(df,
                  col_name,
                  min_val,
                  max_val,
                  use_thousands_separator=True,
                  log_level=logging.DEBUG):
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
        log_level, "Rows kept %s, removed %s rows",
        printing.perc(
            res.shape[0],
            num_rows,
            use_thousands_separator=use_thousands_separator),
        printing.perc(
            num_rows - res.shape[0],
            num_rows,
            use_thousands_separator=use_thousands_separator))
    return res


# #############################################################################
# Plotting
# #############################################################################


def plot_non_na_cols(df, sort=False, ascending=True, max_num=None):
    """
    Plot a diagram describing the non-nans intervals for the columns of df.

    :param df: usual df indexed with times
    :param sort: sort the columns by number of non-nans
    :param ascending:
    :param max_num: max number of columns to plot.
    :return:
    """
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
        _LOG.warning("Plotting only %d columns instead of all %d columns",
                     max_num, df.shape[1])
        dbg.dassert_lte(1, max_num)
        if max_num > df.shape[1]:
            _LOG.warning("Too many columns requested: %d > %d", max_num,
                         df.shape[1])
        df = df.iloc[:, :max_num]
    _LOG.debug("Columns=%d %s", len(df.columns), ", ".join(df.columns))
    _LOG.debug("To plot=\n%s", df.head())
    # Associate each column to a number between 1 and num_cols + 1.
    scale = pd.Series({col: idx + 1 for idx, col in enumerate(df.columns)})
    df *= scale
    num_cols = df.shape[1]
    # Heuristics to find the value of ysize.
    figsize = None
    if True:
        ysize = num_cols * 0.3
        figsize = (20, ysize)
    ax = df.plot(figsize=figsize, legend=False)
    # Force all the yticks to be equal to the column names and to be visible.
    ax.set_yticks(np.arange(num_cols, 0, -1))
    ax.set_yticklabels(reversed(df.columns.tolist()))
    return ax


def plot_heatmap(corr_df,
                 mode,
                 annot="auto",
                 figsize=None,
                 title=None,
                 vmin=None,
                 vmax=None,
                 ax=None):
    """
    Plot a heatmap for a corr / cov df.
    """
    # Sanity check.
    dbg.dassert_eq(corr_df.shape[0], corr_df.shape[1])
    dbg.dassert_lte(corr_df.shape[0], 20)
    if corr_df.shape[0] < 2 or corr_df.shape[1] < 2:
        _LOG.warning("Can't plot heatmap for corr_df with shape=%s",
                     str(corr_df.shape))
        return
    if np.all(np.isnan(corr_df)):
        _LOG.warning("Can't plot heatmap with only nans:\n%s",
                     corr_df.to_string())
        return
    #
    if annot == "auto":
        annot = corr_df.shape[0] < 10
    # Generate a custom diverging colormap.
    cmap = sns.diverging_palette(220, 10, as_cmap=True)
    if figsize is None:
        figsize = (8, 6)
        #figsize = (10, 10)
    if mode == "heatmap":
        # Set up the matplotlib figure.
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        # Draw the heatmap with the mask and correct aspect ratio.
        sns.heatmap(
            corr_df,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            square=True,
            annot=annot,
            fmt=".2f",
            linewidths=.5,
            cbar_kws={"shrink": .5},
            ax=ax)
        ax.set_title(title)
    elif mode == "heatmap_semitriangle":
        # Set up the matplotlib figure.
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        # Generate a mask for the upper triangle.
        mask = np.zeros_like(corr_df, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True
        # Draw the heatmap with the mask and correct aspect ratio.
        sns.heatmap(
            corr_df,
            mask=mask,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            square=True,
            annot=annot,
            fmt=".2f",
            linewidths=.5,
            cbar_kws={"shrink": .5},
            ax=ax)
        ax.set_title(title)
    elif mode == "clustermap":
        dbg.dassert_is(ax, None)
        g = sns.clustermap(
            corr_df,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            linewidths=.5,
            square=True,
            figsize=figsize)
        g.ax_heatmap.set_title(title)
    else:
        raise RuntimeError("Invalid mode='%s'" % mode)


# TODO(saggese): Add an option to mask out the correlation with low pvalues
# http://stackoverflow.com/questions/24432101/correlation-coefficients-and-p-values-for-all-pairs-of-rows-of-a-matrix
def plot_correlation_matrix(df, mode, annot=False, figsize=None, title=None):
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s",
                     str(df.shape))
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
        vmax=1.0)
    return corr_df


def plot_dendogram(df, figsize=None):
    # Look at:
    # ~/.conda/envs/root_longman_20150820/lib/python2.7/site-packages/seaborn/matrix.py
    # https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s",
                     str(df.shape))
        return
    #y = scipy.spatial.distance.pdist(df.values, 'correlation')
    y = df.corr().values
    #z = scipy.cluster.hierarchy.linkage(y, 'single')
    z = scipy.cluster.hierarchy.linkage(y, 'average')
    if figsize is None:
        figsize = (16, 16)
    _ = plt.figure(figsize=figsize)
    scipy.cluster.hierarchy.dendrogram(
        z,
        labels=df.columns.tolist(),
        leaf_rotation=0,
        color_threshold=0,
        orientation='right')


def display_corr_df(df):
    if df is not None:
        df_tmp = df.applymap(lambda x: "%.2f" % x)
        display_df(df_tmp)
    else:
        _LOG.warning("Can't display correlation df since it is None")


# /////////////////////////////////////////////////////////////////////////////
# PCA
# /////////////////////////////////////////////////////////////////////////////


def plot_pca_analysis(ret, plot_explained_variance=False, num_pcs_to_plot=0):
    from sklearn.decomposition import PCA
    # Compute PCA.
    corr = ret.corr(method='pearson')
    pca = PCA()
    pca.fit(ret.fillna(0.0))
    explained_variance = pd.Series(pca.explained_variance_ratio_)
    # Find indices of assets with no nans in the covariance matrix.
    num_non_nan_corr = corr.notnull().sum()
    is_valid = num_non_nan_corr == num_non_nan_corr.max()
    valid_indices = sorted(is_valid[is_valid].index.tolist())
    # Compute eigenvalues / vectors for the subset of the matrix without nans.
    eigenval, eigenvec = np.linalg.eig(
        corr.loc[valid_indices, valid_indices].values)
    # Sort by decreasing eigenvalue.
    ind = eigenval.argsort()[::-1]
    selected_pcs = eigenvec[:, ind]
    pcs = pd.DataFrame(selected_pcs, index=valid_indices)
    lambdas = pd.Series(eigenval[ind])
    #
    if plot_explained_variance:
        title = "Eigenvalues and explained variance vs ordered PCs"
        # Plot explained variance.
        explained_variance.cumsum().plot(
            title=title, lw=5, figsize=(20, 7), ylim=(0, 1))
        # Plot principal component lambda.
        (lambdas / lambdas.max()).plot(color='g', lw=1, kind='bar')
    if num_pcs_to_plot > 0:
        num_cols = 3
        use_subplots = True
        figsize = (20, 7)
        multiple_plots = MultiplePlots(
            num_pcs_to_plot, num_cols, use_subplots, figsize=figsize)
        for i in range(num_pcs_to_plot):
            ax = multiple_plots.get_ax(i)
            temp = pcs.ix[:, i].copy()
            #temp.sort()
            temp.plot(kind='barh', ax=ax, title='PC%s' % i)
    #return pcs, lambdas


def plot_pca_over_time(ret, com):
    corr = pd.ewmcorr(ret.copy(), com=com, min_periods=com)
    # Find when the correlation matrix is all not null.
    start_date = np.argmax(corr.notnull().sum().sum() > 0)
    corr = corr.loc[start_date:]
    corr = corr.fillna(0)
    # Compute rolling eigenvalues.
    val, vec = np.linalg.eig(corr)
    _ = vec
    # Normalize by sum.
    df = pd.DataFrame(val)
    df = df.multiply(1 / df.sum(axis=1), axis="index")
    df.plot(
        figsize=(20, 7),
        cmap='rainbow',
        title='Top eigenvalues by PCs',
        lw=2,
        ylim=(0, 1))
    #return df


def plot_time_distributions(dts, mode, density=True):
    """
    Compute distribution for an array of timestamps `dts`.
    - mode: see below
    """
    dbg.dassert_type_in(dts[0], (datetime.datetime, pd.Timestamp))
    dbg.dassert_in(mode, ("time_of_the_day", "weekday", "minute_of_the_hour",
                          "day_of_the_month", "month_of_the_year", "year"))
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
            labels=False)
        # Count.
        count = pd.Series(vals).value_counts(sort=False)
        # Compute the labels.
        yticks = [
            "%02d:%02d" % (bins[k] / 60, bins[k] % 60) for k in count.index
        ]
    elif mode == "weekday":
        data = [dt.date().weekday() for dt in dts]
        bins = np.arange(0, 7 + 1)
        vals = pd.cut(
            data,
            bins=bins,
            include_lowest=True,
            right=False,
            retbins=False,
            labels=False)
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


# #############################################################################
# Feature selection
# #############################################################################


def _analyze_feature(df, y_var, x_var, use_intercept, nan_mode, x_shift,
                     report_stats):
    _LOG.debug("df=\n%s", df.head(3))
    _LOG.debug("y_var=%s, x_var=%s, use_intercept=%s, nan_mode=%s, x_shift=%s",
               y_var, x_var, use_intercept, nan_mode, x_shift)
    dbg.dassert_isinstance(y_var, str)
    dbg.dassert_isinstance(x_var, str)
    #
    res = collections.OrderedDict()
    res["y_var"] = y_var
    res["x_var"] = x_var
    df_tmp = df[[y_var, x_var]].copy()
    #
    res["x_shift"] = x_shift
    if x_shift != 0:
        df_tmp[x_var] = df_tmp[x_var].shift(x_shift)
    #
    if nan_mode == "drop":
        df_tmp.dropna(inplace=True)
    elif nan_mode == "fill_with_zeros":
        df_tmp.fillna(0.0, inplace=True)
    else:
        raise ValueError("Invalid nan_mode='%s'" % nan_mode)
    res["nan_mode"] = nan_mode
    #
    regr_x_vars = [x_var]
    if use_intercept:
        df_tmp = sm.add_constant(df_tmp)
        regr_x_vars.insert(0, "const")
    res["use_intercept"] = use_intercept
    # Fit.
    reg = sm.OLS(df_tmp[y_var], df_tmp[regr_x_vars])
    model = reg.fit()
    if use_intercept:
        dbg.dassert_eq(len(model.params), 2)
        res["params_const"] = model.params[0]
        res["pvalues_const"] = model.pvalues[0]
        res["params_var"] = model.params[1]
        res["pvalues_var"] = model.pvalues[1]
    else:
        dbg.dassert_eq(len(model.params), 1)
        res["params_var"] = model.params[0]
        res["pvalues_var"] = model.pvalues[0]
    res["nobs"] = model.nobs
    res["condition_number"] = model.condition_number
    res["rsquared"] = model.rsquared
    res["rsquared_adj"] = model.rsquared_adj
    # TODO(gp): Add pnl, correlation, hitrate.
    #
    if report_stats:
        txt = printing.frame(
            "y_var=%s, x_var=%s, use_intercept=%s, nan_mode=%s, x_shift=%s" %
            (y_var, x_var, use_intercept, nan_mode, x_shift))
        _LOG.info("\n%s", txt)
        _LOG.info("model.summary()=\n%s", model.summary())
        sns.regplot(x=df[x_var], y=df[y_var])
        plt.show()
    return res


def analyze_features(df,
                     y_var,
                     x_vars,
                     use_intercept,
                     nan_mode="drop",
                     x_shifts=None,
                     report_stats=False):
    if x_shifts is None:
        x_shifts = [0]
    res_df = []
    for x_var in x_vars:
        _LOG.debug("x_var=%s", x_var)
        for x_shift in x_shifts:
            _LOG.debug("x_shifts=%s", x_shift)
            res_tmp = _analyze_feature(df, y_var, x_var, use_intercept,
                                       nan_mode, x_shift, report_stats)
            res_df.append(res_tmp)
    return pd.DataFrame(res_df)


# #############################################################################
# Printing
# #############################################################################

# TODO(gp): This should go in printing?
#def display(df,
#            threshold=0,
#            remove_index=False,
#            head=None,
#            max_colwidth=100,
#            as_txt=False,
#            return_df=False):
#    df_tmp = df
#    if threshold == 0:
#        df_tmp = remove_const_columns(df_tmp, threshold=threshold)
#    # Head.
#    if head is not None:
#        df_tmp = df_tmp.head(head)
#    # Print.
#    with pd.option_context("display.max_colwidth", max_colwidth):
#        if as_txt:
#            print((df.to_string(index=not remove_index)))
#        else:
#            IPython.core.display.display(
#                IPython.core.display.HTML(df.to_html(index=not remove_index)))
#    # pylint: disable=inconsistent-return-statements
#    if return_df:
#        return df_tmp
