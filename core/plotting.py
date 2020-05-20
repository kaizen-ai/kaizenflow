"""
Import as:

import core.plotting as plot
"""

import logging
import math
from typing import Any, List, Optional, Tuple, Union

import matplotlib.colors as mpl_col
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy as sp
import seaborn as sns
import sklearn.decomposition as skl_dec
import sklearn.metrics as skl_metrics
import sklearn.utils.validation as skl_val
import statsmodels.api as sm

import core.explore as expl
import core.statistics as stats
import helpers.dbg as dbg
import helpers.list as hlist

_LOG = logging.getLogger(__name__)

sns.set_palette("bright")

_ARRAY_LIKE_TYPE = Union[np.array, pd.DataFrame]
_PCA_TYPE = Union[
    skl_dec.PCA, skl_dec.IncrementalPCA, skl_dec.KernelPCA, skl_dec.SparsePCA
]

FIG_SIZE = (20, 5)

_DATETIME_TYPES = [
    "year",
    "month",
    "quarter",
    "weekofyear",
    "dayofweek",
    "hour",
    "minute",
    "second",
]


# #############################################################################
# General dataframe plotting helpers
# #############################################################################


def plot_non_na_cols(
    df: pd.core.frame.DataFrame,
    sort: bool = False,
    ascending: bool = True,
    max_num: Optional[int] = None,
) -> Any:
    """
    Plot a diagram describing the non-nans intervals for the columns of df.

    :param df: usual df indexed with times
    :param sort: sort the columns by number of non-nans
    :param ascending:
    :param max_num: max number of columns to plot.
    """
    # Check that there are no repeated columns.
    # TODO(gp): dassert_no_duplicates
    dbg.dassert_eq(len(hlist.find_duplicates(df.columns.tolist())), 0)
    # Note that the plot assumes that the first column is at the bottom of the
    # Assign 1.0 to all the non-nan value.
    df = df.where(df.isnull(), 1)
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


def plot_categories_count(
    df: pd.core.frame.DataFrame,
    category_column: str,
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
    label: Optional[str] = None,
) -> None:
    """
    Plot countplot of a given `category_column`.

    :df: Df to plot.
    :category_column: Categorial column to subset plots by.
    :figsize: If nothing specified, basic (20,5) used.
    :title: Title for the plot.
    """
    if not figsize:
        figsize = FIG_SIZE
    if not label:
        label = category_column
    num_categories = df[category_column].nunique()
    if num_categories > 10:
        ylen = math.ceil(num_categories / 26) * 5
        figsize = (figsize[0], ylen)
        plt.figure(figsize=figsize)
        ax = sns.countplot(
            y=df[category_column], order=df[category_column].value_counts().index
        )
        ax.set(xlabel=f"Number of {label}s")
        ax.set(ylabel=category_column.lower())
        for p in ax.patches:
            ax.text(
                p.get_width() + 0.1,
                p.get_y() + 0.5,
                str(round((p.get_width()), 2)),
            )
    else:
        plt.figure(figsize=figsize)
        ax = sns.countplot(
            x=df[category_column], order=df[category_column].value_counts().index
        )
        ax.set(xlabel=category_column.lower())
        ax.set(ylabel=f"Number of {label}s")
        for p in ax.patches:
            ax.annotate(p.get_height(), (p.get_x() + 0.35, p.get_height() + 1))
    if not title:
        plt.title(f"Distribution by {category_column}")
    else:
        plt.title(title)
    plt.show()


# #############################################################################
# Time series plotting
# #############################################################################


def plot_timeseries(
    df: pd.core.frame.DataFrame,
    datetime_types: Optional[List[str]],
    column: str,
    ts_column: str,
) -> None:
    """
    Plot timeseries distribution by
    - "year",
    - "month",
    - "quarter",
    - "weekofyear",
    - "dayofweek",
    - "hour",
    - "second"
    unless otherwise provided by `datetime_types`.
    :df: Df to plot.
    :datetime_types: Types of pd.datetime, e.g. "month", "quarter".
    :column: Distribution of which variable to represent.
    :ts_column: Timeseries column.
    """
    unique_rows = expl.drop_duplicates(df=df, subset=[column])
    if not datetime_types:
        datetime_types = _DATETIME_TYPES
    for datetime_type in datetime_types:
        plt.figure(figsize=FIG_SIZE)
        sns.countplot(getattr(unique_rows[ts_column].dt, datetime_type))
        plt.title(f"Distribution by {datetime_type}")
        plt.xlabel(datetime_type, fontsize=12)
        plt.ylabel(f"Quantity of {column}", fontsize=12)
        plt.show()


def plot_timeseries_per_category(
    df: pd.core.frame.DataFrame,
    datetime_types: Optional[List["str"]],
    column: str,
    ts_column: str,
    category_column: str,
    categories: Optional[List[str]] = None,
    top_n: Optional[int] = None,
    figsize: Optional[Tuple[int, int]] = None,
) -> None:
    """
    Plot distribution (where `datetime_types` has the same meaning as in
    plot_headlines) for a given list of categories.

    If `categories` param is not specified, `top_n` must be specified and plots
    will show the `top_n` most popular categories.
    :df: Df to plot.
    :datetime_types: Types of pd.datetime, e.g. "month", "quarter".
    :column: Distribution of which variable to represent.
    :ts_column: Timeseries column.
    :category_column: Categorial column to subset plots by.
    :categories: Categories to represent.
    :top_n: Number of top categories to use, if categories are not specified.
    """
    if not figsize:
        figsize = FIG_SIZE
    unique_rows = expl.drop_duplicates(df=df, subset=[column])
    if top_n:
        categories = (
            df[category_column].value_counts().iloc[:top_n].index.to_list()
        )
    dbg.dassert(categories, "No categories found.")
    if not datetime_types:
        datetime_types = _DATETIME_TYPES
    for datetime_type in datetime_types:
        rows = math.ceil(len(categories) / 3)
        fig, ax = plt.subplots(
            figsize=(FIG_SIZE[0], rows * 4.5),
            ncols=3,
            nrows=rows,
            constrained_layout=True,
        )
        ax = ax.flatten()
        a = iter(ax)
        for category in categories:
            j = next(a)
            # Prepare a subset of data for the current category only.
            rows_by_category = unique_rows.loc[
                unique_rows[categories] == category, :
            ]
            sns.countplot(
                getattr(rows_by_category[ts_column].dt, datetime_type), ax=j
            )
            j.set_ylabel(f"Quantity of {column}")
            j.set_xlabel(datetime_type)
            j.set_xticklabels(j.get_xticklabels(), rotation=45)
            j.set_title(category)
        fig.suptitle(f"Distribution by {datetime_type}")


def plot_cols(
    data: Union[pd.Series, pd.DataFrame],
    colormap: str = "rainbow",
    figsize: Tuple[int, int] = (20, 5),
    mode: Optional[str] = None,
) -> None:
    if isinstance(data, pd.Series):
        data = data.to_frame()
    if mode is None:
        mode = "default"
    elif mode == "renormalize":
        data = data.copy()
        data /= data.std()
    else:
        raise ValueError(f"Unsupported mode `{mode}`")
    data.plot(kind="density", colormap=colormap, figsize=figsize)
    data.plot(colormap=colormap, figsize=figsize)


def plot_autocorrelation(
    signal: Union[pd.Series, pd.DataFrame],
    lags: int = 40,
    zero: bool = False,
    nan_mode: str = "conservative",
    title_prefix: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Plot ACF and PACF of columns.

    https://www.statsmodels.org/stable/_modules/statsmodels/graphics/tsaplots.html#plot_acf
    https://www.statsmodels.org/stable/_modules/statsmodels/tsa/stattools.html#acf
    """
    if isinstance(signal, pd.Series):
        signal = signal.to_frame()
    n_rows = len(signal.columns)
    fig = plt.figure(figsize=(20, 5 * n_rows))
    if title_prefix is None:
        title_prefix = ""
    for idx, col in enumerate(signal.columns):
        if nan_mode == "conservative":
            data = signal[col].fillna(0).dropna()
        else:
            raise ValueError(f"Unsupported nan_mode `{nan_mode}`")
        ax1 = fig.add_subplot(n_rows, 2, 2 * (idx + 1) - 1)
        # Exclude lag zero so that the y-axis does not get squashed.
        acf_title = title_prefix + f"{col} autocorrelation"
        fig = sm.graphics.tsa.plot_acf(
            data, lags=lags, ax=ax1, zero=zero, title=acf_title, **kwargs
        )
        ax2 = fig.add_subplot(n_rows, 2, 2 * (idx + 1))
        pacf_title = title_prefix + f"{col} partial autocorrelation"
        fig = sm.graphics.tsa.plot_pacf(
            data, lags=lags, ax=ax2, zero=zero, title=pacf_title, **kwargs
        )


def plot_spectrum(
    signal: Union[pd.Series, pd.DataFrame],
    nan_mode: str = "conservative",
    title_prefix: Optional[str] = None,
) -> None:
    """
    Plot power spectral density and spectrogram of columns.

    PSD:
      - Estimate the power spectral density using Welch's method.
      - Related to autocorrelation via the Fourier transform (Wiener-Khinchin).
    Spectrogram:
      - From the scipy documentation of spectrogram:
        "Spectrograms can be used as a way of visualizing the change of a
         nonstationary signal's frequency content over time."
    """
    if isinstance(signal, pd.Series):
        signal = signal.to_frame()
    if title_prefix is None:
        title_prefix = ""
    n_rows = len(signal.columns)
    fig = plt.figure(figsize=(20, 5 * n_rows))
    for idx, col in enumerate(signal.columns):
        if nan_mode == "conservative":
            data = signal[col].fillna(0).dropna()
        else:
            raise ValueError(f"Unsupported nan_mode `{nan_mode}`")
        ax1 = fig.add_subplot(n_rows, 2, 2 * (idx + 1) - 1)
        f_pxx, Pxx = sp.signal.welch(data)
        ax1.semilogy(f_pxx, Pxx)
        ax1.set_title(title_prefix + f"{col} power spectral density")
        # TODO(*): Maybe put labels on a shared axis.
        # ax1.set_xlabel("Frequency")
        # ax1.set_ylabel("Power")
        ax2 = fig.add_subplot(n_rows, 2, 2 * (idx + 1))
        f_sxx, t, Sxx = sp.signal.spectrogram(data)
        ax2.pcolormesh(t, f_sxx, Sxx)
        ax2.set_title(title_prefix + f"{col} spectrogram")
        # ax2.set_ylabel("Frequency band")
        # ax2.set_xlabel("Time window")


# #############################################################################
# Correlation-type plots
# #############################################################################


def plot_heatmap(
    corr_df: pd.core.frame.DataFrame,
    mode: str,
    annot: Union[bool, str] = "auto",
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
    vmin: float = -1.0,
    vmax: float = 1.0,
    ax: Optional[plt.axes] = None,
) -> None:
    """
    Plot a heatmap for a corr / cov df.

    :corr_df: Df to plot a heatmap.
    :mode: "heatmap_semitriangle", "heatmap" or "clustermap".
    :annot:
    :figsize: If nothing specified, basic (20,5) used.
    :title: Title for the plot.
    :vmin: Minimum value to anchor the colormap.
    :vmax: Maximum value to anchor the colormap.
    :ax: Axes in which to draw the plot.
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
        figsize = FIG_SIZE
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
def plot_correlation_matrix(
    df: pd.core.frame.DataFrame,
    mode: str,
    annot: Union[bool, str] = False,
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
    method: Optional[str] = None,
    min_periods: Optional[int] = None,
) -> pd.core.frame.DataFrame:
    """
    Compute correlation matrix and plot its heatmap .

    :df: Df to compute correlation matrix and plot a heatmap.
    :mode: "heatmap_semitriangle", "heatmap" or "clustermap".
    :annot:
    :figsize: If nothing specified, basic (20,5) used.
    :title: Title for the plot.
    :method: "pearson", "kendall", "spearman" or callable method of correlation.
    :min_periods: Minimum number of observations required per pair of columns to have
        a valid result. Currently only available for Pearson and Spearman correlation.
    """
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return None
    # Compute the correlation matrix.
    method = method or "pearson"
    corr_df = df.corr(method=method, min_periods=min_periods)
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


def display_corr_df(df: pd.core.frame.DataFrame) -> None:
    """
    Display a correlation df with values with 2 decimal places.
    """
    if df is not None:
        df_tmp = df.applymap(lambda x: "%.2f" % x)
        expl.display_df(df_tmp)
    else:
        _LOG.warning("Can't display correlation df since it is None")


def plot_dendrogram(
    df: pd.core.frame.DataFrame, figsize: Optional[Tuple[int, int]] = None
) -> None:
    """
    Plot a dendrogram.

    A dendrogram is a diagram representing a tree.
    :df: Df to plot a heatmap.
    :figsize: If nothing specified, basic (20,5) used.
    """
    # Look at:
    # ~/.conda/envs/root_longman_20150820/lib/python2.7/site-packages/seaborn/matrix.py
    # https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return
    # y = scipy.spatial.distance.pdist(df.values, 'correlation')
    y = df.corr().values
    # z = scipy.cluster.hierarchy.linkage(y, 'single')
    z = sp.cluster.hierarchy.linkage(y, "average")
    if figsize is None:
        figsize = FIG_SIZE
    _ = plt.figure(figsize=figsize)
    sp.cluster.hierarchy.dendrogram(
        z,
        labels=df.columns.tolist(),
        leaf_rotation=0,
        color_threshold=0,
        orientation="right",
    )


def plot_corr_over_time(
    corr_df: pd.core.frame.DataFrame,
    mode: str,
    annot: bool = False,
    num_cols: int = 4,
) -> None:
    """
    Plot correlation over time.
    """
    timestamps = corr_df.index.get_level_values(0).unique()
    dbg.dassert_lte(len(timestamps), 20)
    # Get the axes.
    fig, axes = expl.get_multiple_plots(
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


def _get_heatmap_mask(corr: pd.DataFrame, mode: str) -> np.ndarray:
    if mode == "heatmap_semitriangle":
        # Generate a mask for the upper triangle.
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True
    elif mode == "heatmap":
        mask = None
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    return mask


def _get_heatmap_colormap() -> mpl_col.LinearSegmentedColormap:
    """
    Generate a custom diverging colormap useful for heatmaps.
    """
    cmap = sns.diverging_palette(220, 10, as_cmap=True)
    return cmap


# #############################################################################
# Eval metrics plots
# #############################################################################


def plot_confusion_heatmap(
    y_true: Union[List[Union[float, int]], np.array],
    y_pred: Union[List[Union[float, int]], np.array],
    return_results: bool = False,
) -> Any:
    """
    Construct and plot a heatmap for a confusion matrix of fact and prediction.

    :y_true: True values.
    :y_pred: Predictions.
    :percentage: to represent values from confusion matrix in percentage or not.
    """
    confusion = skl_metrics.confusion_matrix(y_true, y_pred)
    labels = set(list(y_true))
    df_out = pd.DataFrame(confusion, index=labels, columns=labels)
    df_out_percentage = df_out.apply(lambda x: x / x.sum(), axis=1)
    fig, (ax, ax2) = plt.subplots(figsize=(FIG_SIZE), ncols=2)
    plot_heatmap(
        df_out,
        mode="heatmap",
        vmin=df_out.min().min(),
        vmax=df_out.max().max(),
        ax=ax,
    )
    plot_heatmap(
        df_out_percentage,
        mode="heatmap",
        vmin=df_out_percentage.min().min(),
        vmax=df_out_percentage.max().max(),
        ax=ax2,
    )
    if return_results:
        return df_out, df_out_percentage


def multipletests_plot(
    pvals: pd.Series, threshold: float, method: Optional[str] = None, **kwargs
) -> None:
    """
    Plot adjusted p-values and pass/fail threshold.

    :param pvals: unadjusted p-values
    :param threshold: threshold for adjusted p-values separating accepted and
        rejected hypotheses, e.g., "FWER", or family-wise error rate
    :param method: method for performing p-value adjustment, e.g., "fdr_bh"
    """
    pvals = pvals.sort_values().reset_index(drop=True)
    adj_pvals = stats.multipletests(pvals, method=method)
    plt.plot(pvals, label="pvals", **kwargs)[0]
    plt.plot(adj_pvals, label="adj pvals", **kwargs)
    # Show min adj p-val in text.
    min_adj_pval = adj_pvals[0]
    plt.text(0.1, 0.7, "adj pval=%.3f" % min_adj_pval, fontsize=20)
    plt.text(
        0.1,
        0.6,
        weight="bold",
        fontsize=20,
        **(
            {"s": "PASS", "color": "g"}
            if min_adj_pval <= threshold
            else {"s": "FAIL", "color": "r"}
        ),
    )
    # TODO(*): Force x-ticks at integers.
    plt.axhline(threshold, ls=":", c="k")
    plt.ylim(0, 1)
    plt.legend()


# #############################################################################
# Data count plots.
# #############################################################################


def plot_value_counts(
    counts: pd.Series,
    top_n_to_print: int = 10,
    top_n_to_plot: Optional[int] = None,
    plot_title: Optional[str] = None,
    label: Optional[str] = None,
    figsize: Optional[Tuple[int, int]] = None,
) -> None:
    """
    Plot barplots for value counts and print the values.

    The function is typically used in conjunction with value counts
    from KGification info. If the number of labels is over 20, the plot is
    oriented horizontally and the height of the plot is automatically adjusted.

    :param counts: value counts
    :param top_n_to_print: top N values to show. None for all. 0 for no value.
    :param top_n_to_plot: like top_n_to_print, but for the plot.
    :param plot_title: title of the barplot
    :param label: label of the X axis
    :param figsize: size of the plot
    """
    # Get default values for plot title and label.
    if not figsize:
        figsize = FIG_SIZE
    unique_values = sorted(counts.index.to_list())
    # Display a number of unique values in сolumn.
    print("Number of unique values: %d" % len(unique_values))
    if top_n_to_print == 0:
        # Do not show anything.
        pass
    else:
        counts_tmp = counts.copy()
        counts.sort_values(ascending=False, inplace=True)
        if top_n_to_print is not None:
            dbg.dassert_lte(1, top_n_to_print)
            counts_tmp = counts_tmp[:top_n_to_print]
            print("Up to first %d unique labels:" % top_n_to_print)
        else:
            print("All unique labels:")
        print(counts_tmp)
    # Plot horizontally or vertically, depending on counts number.
    if top_n_to_plot == 0:
        # Do not show anything.
        pass
    else:
        counts_tmp = counts.copy()
        if top_n_to_plot is not None:
            dbg.dassert_lte(1, top_n_to_plot)
            counts_tmp = counts_tmp[:top_n_to_plot]
        if len(counts_tmp) > 20:
            # Plot large number of categories horizontally.
            counts_tmp.sort_values(ascending=True, inplace=True)
            ylen = math.ceil(len(counts_tmp) / 26) * 5
            figsize = (figsize[0], ylen)
            plot_barplot(
                counts_tmp,
                orientation="horizontal",
                plot_title=plot_title,
                figsize=figsize,
                label=label,
            )
        else:
            # Plot small number of categories vertically.
            plot_barplot(
                counts_tmp,
                orientation="vertical",
                plot_title=plot_title,
                figsize=figsize,
                label=label,
            )


def plot_barplot(
    integers: pd.Series,
    orientation: str,
    string_format: str = "%.2f%%",
    plot_title: Optional[str] = None,
    label: Optional[str] = None,
    figsize: Optional[Tuple[int, int]] = None,
) -> None:
    """
    Plot a barplot from value counts, using indices as x-labels.

    :param integers: Series of integers
    :param plot_title: title of the plot
    :param label: label of the X axis
    :param orientation: vertical or horizontal bars
    :param string_format: format of bar annotations
    :param figsize: size of plot
    """

    if figsize is None:
        figsize = FIG_SIZE
    plt.figure(figsize=figsize)
    # Plot vertical bars.
    if orientation == "vertical":
        ax = sns.barplot(x=integers.index, y=integers.values)
        for i, p in enumerate(ax.patches):
            height = p.get_height()
            x, y = p.get_xy()
            # Add percentage annotations to bars.
            ax.annotate(
                string_format % (100 * integers.iloc[i] / integers.sum()),
                (x, y + height + 0.5),
            )
    # Plot horizontal bars.
    elif orientation == "horizontal":
        ax = sns.barplot(y=integers.index, x=integers.values)
        for i, p in enumerate(ax.patches):
            width = p.get_width()
            x, y = p.get_xy()
            # Add percentage annotations to bars.
            ax.annotate(
                string_format % (100 * integers.iloc[i] / integers.sum()),
                (x + width + 0.5, y),
            )
    else:
        dbg.dfatal(message="Invalid plot orientation.")
    if label:
        ax.set(xlabel=label)
    if plot_title:
        plt.title(plot_title)
    plt.show()


# #############################################################################
# PCA
# #############################################################################

# TODO(Julia): Consider wrapping `np.array` outputs into pandas objects.
class PCA:
    def __init__(self, mode: str, **kwargs: Any):
        # TODO(Julia): Find a better name than `linear` since incremental PCA
        #     is linear too.
        dbg.dassert_in(mode, ["linear", "kernel", "sparse", "incremental"])
        if mode == "linear":
            full_mode = "PCA"
        else:
            full_mode = mode.capitalize() + "PCA"
        # TODO(Julia): Either need to make this public, or to stop supporting
        #     sparse PCA and kernel PCA.
        self._pca = getattr(skl_dec, full_mode)(**kwargs)

    def plot(self) -> None:
        skl_val.check_is_fitted(self._pca)

    def plot_explained_variance(self) -> None:
        skl_val.check_is_fitted(self._pca)

    def fit(self, X: _ARRAY_LIKE_TYPE) -> _PCA_TYPE:
        return self._pca.fit(X)

    def partial_fit(
        self, X: _ARRAY_LIKE_TYPE, check_input: bool = True
    ) -> skl_dec.IncrementalPCA:
        if not isinstance(self._pca, skl_dec.IncrementalPCA):
            raise ValueError("`partial_fit` is supported only by incremental PCA")
        return self._pca.partial_fit(X, check_input=check_input)

    def transform(self, X: _ARRAY_LIKE_TYPE) -> np.array:
        return self._pca.transform(X)

    # TODO(Julia): Add `fit_transform`? It is not built in for sparse PCA and
    #     incremental PCA.

    def inverse_transform(self, X: _ARRAY_LIKE_TYPE) -> np.array:
        return self._pca.inverse_transform(X)

    # TODO(Julia): There are no such methods for kernel PCA and sparse PCA.
    def get_covariance(self) -> np.array:
        return self._pca.get_covariance()

    def get_precision(self) -> np.array:
        return self._pca.get_precision()

    # TODO(Julia): It's `n_samples_seen_` for incremental PCA>
    @property
    def n_samples_(self) -> int:
        return self._pca.n_samples_

    @property
    def n_features_(self) -> int:
        return self._pca.n_features_

    @property
    def n_components(self) -> int:
        return self._pca.n_components_

    @property
    def components_(self) -> np.array:
        return self._pca.components_

    @property
    def explained_variance_(self) -> np.array:
        return self._pca.explained_variance_

    @property
    def explained_variance_ratio_(self) -> np.array:
        return self._pca.explained_variance_ratio_

    @property
    def singular_values_(self) -> np.array:
        return self._pca.singular_values_

    @property
    def noise_variance_(self) -> float:
        return self._pca.noise_variance_
