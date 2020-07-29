"""
Import as:

import core.plotting as plot
"""

import calendar
import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib as mpl
import matplotlib.colors as mpl_col
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy as sp
import seaborn as sns
import sklearn.decomposition as skldec
import sklearn.metrics as sklmet
import sklearn.utils.validation as skluv
import statsmodels.api as sm
import statsmodels.regression.rolling as smrr

import core.explore as expl
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dataframe as hdf
import helpers.dbg as dbg
import helpers.list as hlist

_LOG = logging.getLogger(__name__)

_RETURNS_DICT_TYPE = Dict[str, Dict[int, pd.Series]]

_PCA_TYPE = Union[skldec.PCA, skldec.IncrementalPCA]

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
# General plotting helpers
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
    :param max_num: max number of columns to plot
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

    :param df: df to plot
    :param category_column: categorical column to subset plots by
    :param figsize: if nothing specified, basic (20,5) used
    :param title: title for the plot
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


def get_multiple_plots(
    num_plots: int,
    num_cols: int,
    y_scale: Optional[float] = None,
    *args: Any,
    **kwargs: Any,
) -> Tuple[mpl.figure.Figure, np.array]:
    """
    Create figure to accommodate `num_plots` plots.
    The figure is arranged in rows with `num_cols` columns.

    :param num_plots: number of plots
    :param num_cols: number of columns to use in the subplot
    :param y_scale: if not None
    :return: figure and array of axes
    """
    dbg.dassert_lte(1, num_plots)
    dbg.dassert_lte(1, num_cols)
    # Heuristic to find the dimension of the fig.
    if y_scale is not None:
        dbg.dassert_lt(0, y_scale)
        ysize = math.ceil(num_plots / num_cols) * y_scale
        figsize: Optional[Tuple[float, float]] = (20, ysize)
    else:
        figsize = None
    fig, ax = plt.subplots(
        math.ceil(num_plots / num_cols),
        num_cols,
        figsize=figsize,
        *args,
        **kwargs,
    )
    if isinstance(ax, np.ndarray):
        return fig, ax.flatten()
    return fig, ax


# #############################################################################
# Data count plots.
# #############################################################################


def plot_value_counts(
    srs: pd.Series, dropna: bool = True, *args: Any, **kwargs: Any
) -> None:
    """
    Plot barplots for the counts of a series and print the values.

    Same interface as plot_count_series() but computing the count of the given
    series `srs`.
    """
    # Compute the counts.
    counts = srs.value_counts(dropna=dropna)
    # Plot.
    return plot_counts(counts, *args, **kwargs)


def plot_counts(
    counts: pd.Series,
    top_n_to_print: int = 10,
    top_n_to_plot: Optional[int] = None,
    plot_title: Optional[str] = None,
    label: Optional[str] = None,
    figsize: Optional[Tuple[int, int]] = None,
    rotation: int = 0,
) -> None:
    """
    Plot barplots for series containing counts and print the values.

    If the number of labels is over 20, the plot is oriented horizontally
    and the height of the plot is automatically adjusted.

    :param counts: series to plot value counts for
    :param top_n_to_print: top N values by count to print. None for all. 0 for
        no values
    :param top_n_to_plot: like top_n_to_print, but for the plot
    :param plot_title: title of the barplot
    :param label: label of the X axis
    :param figsize: size of the plot
    :param rotation: rotation of xtick labels
    """
    # Get default values for plot title and label.
    if not figsize:
        figsize = FIG_SIZE
    # Display a number of unique values in сolumn.
    print("Number of unique values: %d" % counts.index.nunique())
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
        # Subset N values to plot.
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
                title=plot_title,
                figsize=figsize,
                xlabel=label,
                rotation=rotation,
            )
        else:
            # Plot small number of categories vertically.
            plot_barplot(
                counts_tmp,
                orientation="vertical",
                title=plot_title,
                figsize=figsize,
                xlabel=label,
                rotation=rotation,
            )


def plot_barplot(
    srs: pd.Series,
    orientation: str = "vertical",
    annotation_mode: str = "pct",
    string_format: str = "%.2f",
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    unicolor: bool = False,
    color_palette: Optional[List[Tuple[float, float, float]]] = None,
    figsize: Optional[Tuple[int, int]] = None,
    rotation: int = 0,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot a barplot.

    :param srs: pd.Series
    :param orientation: vertical or horizontal bars
    :param annotation_mode: `pct` or `value`
    :param string_format: format of bar annotations
    :param title: title of the plot
    :param xlabel: label of the X axis
    :param unicolor: if True, plot all bars in neutral blue color
    :param color_palette: color palette
    :param figsize: size of plot
    :param rotation: rotation of xtick labels
    :param ax: axes
    """

    def _get_annotation_loc(
        x_: float, y_: float, height_: float, width_: float
    ) -> Tuple[float, float]:
        if orientation == "vertical":
            return x_, y_ + max(height_, 0)
        if orientation == "horizontal":
            return x_ + max(width_, 0), y_
        raise ValueError("Invalid orientation='%s'" % orientation)

    if figsize is None:
        figsize = FIG_SIZE
    # Choose colors.
    if unicolor:
        color = sns.color_palette("muted")[0]
    else:
        color_palette = color_palette or sns.diverging_palette(10, 133, n=2)
        color = (srs > 0).map({True: color_palette[-1], False: color_palette[0]})
    # Plot.
    if orientation == "vertical":
        kind = "bar"
    elif orientation == "horizontal":
        kind = "barh"
    else:
        raise ValueError("Invalid orientation='%s'" % orientation)
    ax = ax or plt.gca()
    srs.plot(
        kind=kind, color=color, rot=rotation, title=title, ax=ax, figsize=figsize
    )
    # Add annotations to bars.
    if annotation_mode == "pct":
        annotations = srs * 100 / srs.sum()
        string_format = string_format + "%%"
        annotations = annotations.apply(lambda z: string_format % z)
    elif annotation_mode == "value":
        annotations = srs.apply(lambda z: string_format % z)
    else:
        raise ValueError("Invalid annotations_mode='%s'" % annotation_mode)
    #
    for i, p in enumerate(ax.patches):
        height = p.get_height()
        width = p.get_width()
        x, y = p.get_xy()
        annotation_loc = _get_annotation_loc(x, y, height, width)
        ax.annotate(annotations.iloc[i], annotation_loc)
    if xlabel:
        ax.set(xlabel=xlabel)


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

    :param df: df to plot
    :param datetime_types: types of pd.datetime, e.g. "month", "quarter"
    :param column: distribution of which variable to represent
    :param ts_column: timeseries column
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
    :param df: df to plot
    :param datetime_types: types of pd.datetime, e.g. "month", "quarter"
    :param column: distribution of which variable to represent
    :param ts_column: timeseries column
    :param category_column: categorical column to subset plots by
    :param categories: categories to represent
    :param top_n: number of top categories to use, if categories are not specified
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
    mode: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = None,
    figsize: Optional[Tuple[float, float]] = (20, 10),
) -> None:
    """
    Plot lineplot and density plot for the given dataframe.

    :param data: data to plot
    :param colormap: preferred colors
    :param mode: "renormalize" or "default"
    :param axes: pair of axes for plot over time and density plot
    :param figsize: matplotlib figsize. Default is `(20, 10)`. If `None`, uses
        notebook default parameters
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    if axes is None:
        _, axes = plt.subplots(2, ncols=1, figsize=figsize)
    if mode is None or mode == "default":
        pass
    elif mode == "renormalize":
        data = data.copy()
        data /= data.std()
    else:
        raise ValueError(f"Unsupported mode `{mode}`")
    data.plot(kind="density", colormap=colormap, ax=axes[0])
    data.plot(colormap=colormap, ax=axes[1])


def plot_autocorrelation(
    signal: Union[pd.Series, pd.DataFrame],
    lags: int = 40,
    zero: bool = False,
    nan_mode: str = "conservative",
    fft: bool = False,
    title_prefix: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = [[None, None]],
    **kwargs: Any,
) -> None:
    """
    Plot ACF and PACF of columns.

    https://www.statsmodels.org/stable/_modules/statsmodels/graphics/tsaplots.html#plot_acf
    https://www.statsmodels.org/stable/_modules/statsmodels/tsa/stattools.html#acf
    """
    if isinstance(signal, pd.Series):
        signal = signal.to_frame()
    nrows = len(signal.columns)
    if axes == [[None, None]]:
        _, axes = plt.subplots(nrows=nrows, ncols=2, figsize=(20, 5 * nrows))
        if axes.size == 2:
            axes = [axes]
    if title_prefix is None:
        title_prefix = ""
    for idx, col in enumerate(signal.columns):
        if nan_mode == "conservative":
            data = signal[col].fillna(0).dropna()
        else:
            raise ValueError(f"Unsupported nan_mode `{nan_mode}`")
        ax1 = axes[idx][0]
        # Exclude lag zero so that the y-axis does not get squashed.
        acf_title = title_prefix + f"{col} autocorrelation"
        _ = sm.graphics.tsa.plot_acf(
            data, lags=lags, fft=fft, ax=ax1, zero=zero, title=acf_title, **kwargs
        )
        ax2 = axes[idx][1]
        pacf_title = title_prefix + f"{col} partial autocorrelation"
        _ = sm.graphics.tsa.plot_pacf(
            data, lags=lags, ax=ax2, zero=zero, title=pacf_title, **kwargs,
        )


def plot_spectrum(
    signal: Union[pd.Series, pd.DataFrame],
    nan_mode: str = "conservative",
    title_prefix: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = [[None, None]],
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
    nrows = len(signal.columns)
    if axes == [[None, None]]:
        _, axes = plt.subplots(nrows=nrows, ncols=2, figsize=(20, 5 * nrows))
        if axes.size == 2:
            axes = [axes]
    for idx, col in enumerate(signal.columns):
        if nan_mode == "conservative":
            data = signal[col].fillna(0).dropna()
        else:
            raise ValueError(f"Unsupported nan_mode `{nan_mode}`")
        ax1 = axes[idx][0]
        f_pxx, Pxx = sp.signal.welch(data)
        ax1.semilogy(f_pxx, Pxx)
        ax1.set_title(title_prefix + f"{col} power spectral density")
        # TODO(*): Maybe put labels on a shared axis.
        # ax1.set_xlabel("Frequency")
        # ax1.set_ylabel("Power")
        ax2 = axes[idx][1]
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

    :param corr_df: df to plot a heatmap
    :param mode: "heatmap_semitriangle", "heatmap" or "clustermap"
    :param annot: determines whether to use annotations
    :param figsize: if nothing specified, basic (20,5) used
    :param title: title for the plot
    :param vmin: minimum value to anchor the colormap
    :param vmax: maximum value to anchor the colormap
    :param ax: axes in which to draw the plot
    """
    # Sanity check.
    dbg.dassert_eq(corr_df.shape[0], corr_df.shape[1])
    if corr_df.shape[0] > 20:
        _LOG.warning("The corr_df.shape[0]='%s' > 20", corr_df.shape[0])
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
    Compute correlation matrix and plot its heatmap.

    :param df: Df to compute correlation matrix and plot a heatmap
    :param mode: "heatmap_semitriangle", "heatmap" or "clustermap"
    :param annot: determines whether to use annotations
    :param figsize: if nothing specified, basic (20,5) used
    :param title: title for the plot
    :param method: "pearson", "kendall", "spearman" or callable method of correlation
    :param min_periods: minimum number of observations required per pair of columns to have
        a valid result; currently only available for Pearson and Spearman correlation
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

    :param df: df to plot a heatmap
    :param figsize: if nothing specified, basic (20,5) used
    """
    # Look at:
    # ~/.conda/envs/root_longman_20150820/lib/python2.7/site-packages/seaborn/matrix.py
    # https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
    # Drop constant columns.
    constant_cols = df.columns[(df.diff().iloc[1:] == 0).all()]
    if not constant_cols.empty:
        _LOG.warning("Excluding constant columns: %s", constant_cols.tolist())
        df = df.drop(columns=constant_cols)
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return
    y = df.corr().values
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
    if len(timestamps) > 20:
        _LOG.warning("The first level of index length='%s' > 20", len(timestamps))
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


class PCA:
    def __init__(self, mode: str, **kwargs: Any):
        if mode == "standard":
            self.pca = skldec.PCA(**kwargs)
        elif mode == "incremental":
            self.pca = skldec.IncrementalPCA(**kwargs)
        else:
            raise ValueError("Invalid mode='%s'" % mode)

    def plot_components(
        self, num_components: Optional[int] = None, num_cols: int = 4
    ) -> None:
        """
        Plot principal components.

        :param num_components: number of top components to plot
        :param num_cols: number of columns to use in the subplot
        """
        skluv.check_is_fitted(self.pca)
        pcs = pd.DataFrame(self.pca.components_)
        max_pcs = self.pca.components_.shape[0]
        num_components = self._get_num_pcs_to_plot(num_components, max_pcs)
        _LOG.info("num_components=%s", num_components)
        _, axes = get_multiple_plots(
            num_components, num_cols=num_cols, sharex=True, sharey=True
        )
        plt.suptitle("Principal components")
        for i in range(num_components):
            pc = pcs.iloc[i, :]
            pc.plot(
                kind="barh", ax=axes[i], title="PC%s" % i, edgecolor="tab:blue"
            )

    def plot_explained_variance(self) -> None:
        skluv.check_is_fitted(self.pca)
        explained_variance_ratio = pd.Series(self.pca.explained_variance_ratio_)
        eigenvals = pd.Series(self.pca.explained_variance_)
        # Plot explained variance.
        explained_variance_ratio.cumsum().plot(
            title="Explained variance ratio", lw=5, ylim=(0, 1)
        )
        (eigenvals / eigenvals.max()).plot(color="g", kind="bar", rot=0)

    def fit(self, X: pd.DataFrame, standardize: bool = False) -> _PCA_TYPE:
        if standardize:
            X = (X - X.mean()) / X.std()
        return self.pca.fit(X)

    @staticmethod
    def _get_num_pcs_to_plot(num_pcs_to_plot: Optional[int], max_pcs: int) -> int:
        """
        Get the number of principal components to plot.
        """
        if num_pcs_to_plot is None:
            num_pcs_to_plot = max_pcs
            _LOG.warning("Plotting all %s components", num_pcs_to_plot)
        dbg.dassert_lte(1, num_pcs_to_plot)
        dbg.dassert_lte(num_pcs_to_plot, max_pcs)
        return num_pcs_to_plot


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

    :param y_true: true values
    :param y_pred: predictions
    :param return_results: determines whether to return result dataframes
    """
    confusion = sklmet.confusion_matrix(y_true, y_pred)
    labels = set(list(y_true))
    df_out = pd.DataFrame(confusion, index=labels, columns=labels)
    df_out_percentage = df_out.apply(lambda x: x / x.sum(), axis=1)
    _, (ax, ax2) = plt.subplots(figsize=(FIG_SIZE), ncols=2)
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
    pvals: pd.Series,
    threshold: float,
    adj_pvals: Optional[Union[pd.Series, pd.DataFrame]] = None,
    num_cols: Optional[int] = None,
    method: Optional[str] = None,
    suptitle: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """
    Plot adjusted p-values and pass/fail threshold.

    :param pvals: unadjusted p-values
    :param threshold: threshold for adjusted p-values separating accepted and
        rejected hypotheses, e.g., "FWER", or family-wise error rate
    :param adj_pvals: adjusted p-values, if provided, will be used instead
        calculating inside the function
    :param num_cols: number of columns in multiplotting
    :param method: method for performing p-value adjustment, e.g., "fdr_bh"
    :param suptitle: overall title of all plots
    """
    if adj_pvals is None:
        pval_series = pvals.dropna().sort_values().reset_index(drop=True)
        adj_pvals = stats.multipletests(pval_series, method=method).to_frame()
    else:
        pval_series = pvals.dropna()
        if isinstance(adj_pvals, pd.Series):
            adj_pvals = adj_pvals.to_frame()
    num_cols = num_cols or 1
    adj_pvals = adj_pvals.dropna(axis=1, how="all")
    _, ax = get_multiple_plots(
        adj_pvals.shape[1],
        num_cols=num_cols,
        sharex=False,
        sharey=True,
        y_scale=5,
    )
    if not isinstance(ax, np.ndarray):
        ax = [ax]
    for i, col in enumerate(adj_pvals.columns):
        mask = adj_pvals[col].notna()
        adj_pval = adj_pvals.loc[mask, col].sort_values().reset_index(drop=True)
        ax[i].plot(
            pval_series.loc[mask].sort_values().reset_index(drop=True),
            label="pvals",
            **kwargs,
        )
        ax[i].plot(adj_pval, label="adj pvals", **kwargs)
        # Show min adj p-val in text.
        min_adj_pval = adj_pval.iloc[0]
        ax[i].text(0.1, 0.7, "adj pval=%.3f" % min_adj_pval, fontsize=20)
        ax[i].text(
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
        ax[i].set_title(col)
        ax[i].axhline(threshold, ls=":", c="k")
        ax[i].set_ylim(0, 1)
        ax[i].legend()
    plt.suptitle(suptitle, x=0.5105, y=1.01, fontsize=15)
    plt.tight_layout()


# #############################################################################
# Model evaluation
# #############################################################################


def plot_cumulative_returns(
    cumulative_rets: pd.Series,
    mode: str,
    unit: str = "ratio",
    benchmark_series: Optional[pd.Series] = None,
    title_suffix: Optional[str] = None,
    ax: Optional[mpl.axes.Axes] = None,
    plot_zero_line: bool = True,
) -> None:
    """
    Plot cumulative returns.

    :param cumulative_rets: log or pct cumulative returns
    :param mode: log or pct, used to choose plot title
    :param unit: "ratio", "%" or "bps", both input series are rescaled
        appropriately
    :param benchmark_series: additional series to plot
    :param title_suffix: suffix added to the title
    :param ax: axes
    :param plot_zero_line: whether to plot horizontal line at 0
    """
    title_suffix = title_suffix or ""
    scale_coeff = _choose_scaling_coefficient(unit)
    cumulative_rets = cumulative_rets * scale_coeff
    #
    if mode == "log":
        title = "Cumulative log returns"
    elif mode == "pct":
        title = "Cumulative returns"
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    label = cumulative_rets.name or "returns"
    #
    ax = ax or plt.gca()
    cumulative_rets.plot(ax=ax, title=f"{title}{title_suffix}", label=label)
    if benchmark_series is not None:
        benchmark_series = benchmark_series.loc[
            cumulative_rets.index[0] : cumulative_rets.index[-1]
        ]
        benchmark_series = benchmark_series * scale_coeff
        bs_label = benchmark_series.name or "benchmark_series"
        benchmark_series.plot(ax=ax, label=bs_label, color="grey")
    if plot_zero_line:
        ax.axhline(0, linestyle="--", linewidth=0.8, color="black")
    ax.set_ylabel(unit)
    ax.legend()


def plot_rolling_annualized_volatility(
    srs: pd.Series,
    tau: float,
    min_periods: Optional[int] = None,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
    unit: str = "ratio",
    trim_index: Optional[bool] = False,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot rolling annualized volatility.

    :param srs: input series
    :param tau: argument as for sigp.compute_rolling_std
    :param min_periods: argument as for sigp.compute_rolling_std
    :param min_depth: argument as for sigp.compute_rolling_std
    :param max_depth: argument as for sigp.compute_rolling_std
    :param p_moment: argument as for sigp.compute_rolling_std
    :param unit: "ratio", "%" or "bps" scaling coefficient
        "Exchange:Kibot_symbol"
        "Exchange:Exchange_symbol"
    :param trim_index: start plot at original index if True
    :param ax: axes
    """
    min_periods = min_periods or tau
    srs = hdf.apply_nan_mode(srs, mode="fill_with_zero")
    # Calculate rolling volatility.
    rolling_volatility = sigp.compute_rolling_std(
        srs, tau, min_periods, min_depth, max_depth, p_moment
    )
    # Annualize rolling volatility.
    ppy = hdf.infer_sampling_points_per_year(srs)
    annualized_rolling_volatility = np.sqrt(ppy) * rolling_volatility
    # Remove leading `NaNs`.
    first_valid_index = annualized_rolling_volatility.first_valid_index()
    annualized_rolling_volatility = annualized_rolling_volatility.loc[
        first_valid_index:
    ]
    # Rescale according to desired output units.
    scale_coeff = _choose_scaling_coefficient(unit)
    annualized_rolling_volatility *= scale_coeff
    # Calculate whole-period target volatility.
    annualized_volatility = fin.compute_annualized_volatility(srs)
    annualized_volatility *= scale_coeff
    # Plot.
    ax = ax or plt.gca()
    ax.plot(
        annualized_rolling_volatility, label="annualized rolling volatility",
    )
    ax.axhline(
        annualized_volatility,
        linestyle="--",
        linewidth=2,
        color="green",
        label="average annualized volatility",
    )
    ax.axhline(0, linewidth=0.8, color="black")
    ax.set_title(f"Annualized rolling volatility ({unit})")
    # Start plot from original index if specified.
    if not trim_index:
        ax.set_xlim([min(srs.index), max(srs.index)])
    else:
        ax.set_xlim(
            annualized_rolling_volatility.index[0],
            annualized_rolling_volatility.index[-1],
        )
    ax.set_ylabel(unit)
    ax.set_xlabel("period")
    ax.legend()


def plot_rolling_annualized_sharpe_ratio(
    srs: pd.Series,
    tau: float,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
    ci: float = 0.95,
    title_suffix: Optional[str] = None,
    trim_index: Optional[bool] = False,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot rolling annualized Sharpe ratio.

    :param srs: input series
    :param tau: argument as for sigp.compute_smooth_moving_average
    :param min_depth: argument as for sigp.compute_smooth_moving_average
    :param max_depth: argument as for sigp.compute_smooth_moving_average
    :param p_moment: argument as for sigp.compute_smooth_moving_average
    :param ci: confidence interval
    :param title_suffix: suffix added to the title
    :param trim_index: start plot at original index if True
    :param ax: axes
    """
    title_suffix = title_suffix or ""
    srs = hdf.apply_nan_mode(srs, mode="fill_with_zero")
    min_periods = tau * max_depth
    rolling_sharpe = sigp.compute_rolling_annualized_sharpe_ratio(
        srs,
        tau,
        min_periods=min_periods,
        min_depth=min_depth,
        max_depth=max_depth,
        p_moment=p_moment,
    )
    # Remove leading `NaNs`.
    first_valid_index = rolling_sharpe.first_valid_index()
    rolling_sharpe = rolling_sharpe.loc[first_valid_index:]
    # Prepare for plotting SE band.
    z = sp.stats.norm.ppf((1 - ci) / 2)
    rolling_sharpe["sr-z*se"] = (
        rolling_sharpe["annualized_SR"] + z * rolling_sharpe["annualized_SE(SR)"]
    )
    rolling_sharpe["sr+z*se"] = (
        rolling_sharpe["annualized_SR"] - z * rolling_sharpe["annualized_SE(SR)"]
    )
    # Plot.
    ax = rolling_sharpe["annualized_SR"].plot(
        ax=ax, title=f"Annualized rolling Sharpe ratio{title_suffix}", label="SR"
    )
    ax.fill_between(
        rolling_sharpe.index,
        rolling_sharpe["sr-z*se"],
        rolling_sharpe["sr+z*se"],
        alpha=0.4,
        label=f"{100*ci:.2f}% confidence interval",
    )
    mean_sharpe_ratio = (
        rolling_sharpe["annualized_SR"]
        .replace([np.inf, -np.inf], value=np.nan)
        .mean()
    )
    ax = ax or plt.gca()
    ax.axhline(
        mean_sharpe_ratio,
        linestyle="--",
        linewidth=2,
        color="green",
        label="average SR",
    )
    ax.axhline(0, linewidth=0.8, color="black", label="0")
    # Start plot from original index if specified.
    if not trim_index:
        ax.set_xlim([min(srs.index), max(srs.index)])
    ax.set_ylabel("annualized SR")
    ax.legend()


def plot_yearly_barplot(
    log_rets: pd.Series,
    unit: str = "ratio",
    unicolor: bool = False,
    orientation: str = "vertical",
    figsize: Optional[Tuple[int, int]] = None,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot a barplot of log returns statistics by year.

    :param log_rets: input series of log returns
    :param unit: "ratio", "%" or "bps" scaling coefficient
    :param unicolor: if True, plot all bars in neutral blue color
    :param orientation: vertical or horizontal bars
    :param figsize: size of plot
    :param ax: axes
    """
    scale_coeff = _choose_scaling_coefficient(unit)
    yearly_log_returns = log_rets.resample("Y").sum()
    yearly_pct_returns = fin.convert_log_rets_to_pct_rets(yearly_log_returns)
    yearly_returns = yearly_pct_returns * scale_coeff
    yearly_returns.index = yearly_returns.index.year
    ax = ax or plt.gca()
    plot_barplot(
        yearly_returns,
        annotation_mode="value",
        orientation=orientation,
        title=f"Annual returns ({unit})",
        unicolor=unicolor,
        ax=ax,
        figsize=figsize,
    )
    if orientation == "vertical":
        xlabel = "year"
        ylabel = unit
    elif orientation == "horizontal":
        xlabel = unit
        ylabel = "year"
    else:
        raise ValueError("Invalid orientation='%s'" % orientation)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)


def plot_monthly_heatmap(
    log_rets: pd.Series, unit: str = "ratio", ax: Optional[mpl.axes.Axes] = None
) -> None:
    """
    Plot a heatmap of log returns statistics by year and month.

    :param log_rets: input series of log returns
    :param unit: "ratio", `%` or "bps" scaling coefficient
    :param ax: axes
    """
    scale_coeff = _choose_scaling_coefficient(unit)
    ax = ax or plt.gca()
    monthly_pct_spread = _calculate_year_to_month_spread(log_rets)
    monthly_spread = monthly_pct_spread * scale_coeff
    cmap = sns.diverging_palette(10, 133, as_cmap=True)
    sns.heatmap(monthly_spread, center=0, cmap=cmap, annot=True, fmt=".2f", ax=ax)
    ax.set_title(f"Monthly returns ({unit})")
    ax.tick_params(axis="y", rotation=0)


def plot_pnl(
    pnls: Dict[int, pd.Series],
    title: Optional[str] = None,
    colormap: Optional[str] = None,
    figsize: Optional[Tuple[int]] = None,
    start_date: Optional[Union[str, pd.Timestamp]] = None,
    end_date: Optional[Union[str, pd.Timestamp]] = None,
    nan_mode: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot pnls for dict of pnl time series.

    :param pnls: dict of pnl time series
    :param title: plot title
    :param colormap: matplotlib colormap name
    :param figsize: size of plot
    :param start_date: left limit value of the X axis
    :param end_date: right limit value of the X axis
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param xlabel: label of the X axis
    :param ylabel: label of the Y axis
    :param ax: axes
    """
    title = title or ""
    colormap = colormap or "rainbow"
    nan_mode = nan_mode or "drop"
    xlabel = xlabel or None
    ylabel = ylabel or None
    fstr = "{col} (SR={sr})"
    ax = ax or plt.gca()
    #
    pnls_notna = {}
    empty_srs = []
    for key, srs in pnls.items():
        srs = hdf.apply_nan_mode(srs, mode=nan_mode)
        if srs.dropna().empty:
            empty_srs.append(key)
        else:
            pnls_notna[key] = srs
    if empty_srs:
        _LOG.warning(
            "Empty input series were dropped: '%s'",
            ", ".join(empty_srs.astype(str)),
        )
    df_plot = pd.concat(pnls_notna, axis=1)
    # Compute sharpe ratio for every time series.
    sharpe_ratio = {
        key: stats.compute_annualized_sharpe_ratio(srs)
        for key, srs in pnls.items()
    }
    sharpe_ratio = pd.Series(sharpe_ratio)
    sharpe_cols = [
        [round(sr, 1), df_plot.columns[i]] for i, sr in enumerate(sharpe_ratio)
    ]
    # Change column names and order to column names with sharpe ratio.
    df_plot.columns = [
        fstr.format(col=str(item[1]), sr=str(item[0])) for item in sharpe_cols
    ]
    sharpe_cols = sorted(sharpe_cols, key=lambda x: x[0], reverse=True)
    sorted_names = [
        fstr.format(col=str(item[1]), sr=str(item[0])) for item in sharpe_cols
    ]
    df_plot = df_plot.reindex(sorted_names, axis=1)
    # Plotting the dataframe without dropping `NaN`s in each column results in
    # a missing line for some of the pnls. To avoid it, plot by column.
    cmap = mpl.cm.get_cmap(colormap)
    colors = np.linspace(0, 1, df_plot.shape[1])
    colors = [cmap(c) for c in colors]
    for color, col in zip(colors, df_plot.columns):
        df_plot[col].cumsum().dropna().plot(ax=ax, color=color, figsize=figsize)
    # Setting fixed borders of x-axis.
    left_lim = start_date or min(df_plot.index)
    right_lim = end_date or max(df_plot.index)
    ax.set_xlim([left_lim, right_lim])
    # Formatting.
    ax.set_title(title, fontsize=20)
    ax.set_ylabel(ylabel, fontsize=20)
    ax.set_xlabel(xlabel, fontsize=20)
    ax.legend(prop=dict(size=13), loc="upper left")


def plot_drawdown(
    log_rets: pd.Series,
    unit: str = "%",
    title_suffix: Optional[str] = None,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot drawdown.

    :param log_rets: log returns
    :param unit: `ratio`, `%`, input series is rescaled appropriately
    :param title_suffix: suffix added to the title
    :param ax: axes
    """
    title_suffix = title_suffix or ""
    scale_coeff = _choose_scaling_coefficient(unit)
    drawdown = -scale_coeff * fin.compute_perc_loss_from_high_water_mark(log_rets)
    label = drawdown.name or "drawdown"
    title = f"Drawdown ({unit})"
    ax = ax or plt.gca()
    drawdown.plot(ax=ax, label="_nolegend_", color="b", linewidth=3.5)
    drawdown.plot.area(
        ax=ax, title=f"{title}{title_suffix}", label=label, color="b", alpha=0.3
    )
    ax.set_ylim(top=0)
    ax.set_ylabel(unit)
    ax.legend()


def plot_holdings(
    holdings: pd.Series, unit: str = "ratio", ax: Optional[mpl.axes.Axes] = None
) -> None:
    """
    Plot holdings, average holdings and average holdings by month.

    :param holdings: pnl series to plot
    :param unit: "ratio", "%" or "bps" scaling coefficient
    :param ax: axes in which to draw the plot
    """
    ax = ax or plt.gca()
    scale_coeff = _choose_scaling_coefficient(unit)
    holdings = scale_coeff * holdings
    holdings.plot(linewidth=1, ax=ax, label="holdings")
    holdings.resample("M").mean().plot(
        linewidth=2.5, ax=ax, label="average holdings by month"
    )
    ax.axhline(
        holdings.mean(),
        linestyle="--",
        color="green",
        label="average holdings, overall",
    )
    ax.set_ylabel(unit)
    ax.legend()
    ax.set_title(f"Total holdings ({unit})")


def plot_qq(
    srs: pd.Series,
    ax: Optional[mpl.axes.Axes] = None,
    dist: Optional[str] = None,
    nan_mode: Optional[str] = None,
) -> None:
    """
    Plot ordered values against theoretical quantiles of the given distribution.

    :param srs: data to plot
    :param ax: axes in which to draw the plot
    :param dist: distribution name
    :param nan_mode: argument for hdf.apply_nan_mode()
    """
    dist = dist or "norm"
    ax = ax or plt.gca()
    nan_mode = nan_mode or "drop"
    x_plot = hdf.apply_nan_mode(srs, mode=nan_mode)
    sp.stats.probplot(x_plot, dist=dist, plot=ax)
    ax.set_title(f"{dist} probability plot")


def plot_turnover(
    positions: pd.Series, unit: str = "ratio", ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot turnover, average turnover by month and overall average turnover.

    :param positions: series of positions to plot
    :param unit: "ratio", "%" or "bps" scaling coefficient
    :param ax: axes in which to draw the plot
    """
    ax = ax or plt.gca()
    scale_coeff = _choose_scaling_coefficient(unit)
    turnover = fin.compute_turnover(positions)
    turnover = scale_coeff * turnover
    turnover.plot(linewidth=1, ax=ax, label="turnover")
    turnover.resample("M").mean().plot(
        linewidth=2.5, ax=ax, label="average turnover by month"
    )
    ax.axhline(
        turnover.mean(),
        linestyle="--",
        color="green",
        label="average turnover, overall",
    )
    ax.set_ylabel(unit)
    ax.legend()
    ax.set_title(f"Turnover ({unit})")


def plot_allocation(
    position_df: pd.DataFrame,
    config: Dict[str, Any],
    figsize: Optional[Tuple[int, int]] = None,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """
    Plot position allocations over time.

    :param position_df: dataframe with position time series
    :param config: information about time series
    :param figsize: size of plot
    :param ax: axes
    """
    ax = ax or plt.gca()
    figsize = figsize or (20, 5)
    fstr = "{key} [{tag}]"
    labels = [
        fstr.format(key=str(key), tag=config[key]["tag"]) for key in config.keys()
    ]
    position_df_plot = position_df.copy()
    position_df_plot.columns = labels
    position_df_plot.plot(ax=ax, figsize=figsize)
    ax.set_title(
        f"Portfolio allocation over time; {position_df.shape[1]} positions"
    )
    ax.set_xlabel("period")
    ax.legend()


def plot_rolling_beta(
    rets: pd.Series,
    benchmark_rets: pd.Series,
    window: Optional[int],
    nan_mode: str = "leave_unchanged",
    figsize: Optional[Tuple[int, int]] = None,
    ax: Optional[mpl.axes.Axes] = None,
    **kwargs: Any,
) -> None:
    """
    Regress returns against benchmark series and plot rolling beta.

    :param rets: returns
    :param benchmark_rets: benchmark returns
    :param window: window size
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param figsize: figure size
    :param ax: axis
    :param kwargs: kwargs for statsmodels.regression.rolling.RollingOLS
    """
    dbg.dassert_strictly_increasing_index(rets)
    dbg.dassert_strictly_increasing_index(benchmark_rets)
    rets = hdf.apply_nan_mode(rets, nan_mode)
    benchmark_rets = hdf.apply_nan_mode(benchmark_rets, nan_mode)
    benchmark_rets = benchmark_rets.reindex_like(rets, method="ffill")
    #
    ax = ax or plt.gca()
    benchmark_name = benchmark_rets.name
    benchmark_rets = sm.add_constant(benchmark_rets)
    model = smrr.RollingOLS(rets, benchmark_rets, window=window, **kwargs)
    res = model.fit()
    beta = res.params[benchmark_name]
    beta.plot(
        ax=ax, figsize=figsize, title=f"Beta estimate using {benchmark_name}"
    )
    ax.set_xlabel("period")
    ax.set_ylabel("beta")


def _choose_scaling_coefficient(unit: str) -> int:
    if unit == "%":
        scale_coeff = 100
    elif unit == "bps":
        scale_coeff = 10000
    elif unit == "ratio":
        scale_coeff = 1
    else:
        raise ValueError("Invalid unit='%s'" % unit)
    return scale_coeff


def _calculate_year_to_month_spread(log_rets: pd.Series) -> pd.DataFrame:
    """
    Calculate log returns statistics by year and month.

    :param log_rets: input series of log returns
    :return: dataframe of log returns with years on y-axis and
        months on x-axis
    """
    srs_name = log_rets.name or 0
    log_rets_df = pd.DataFrame(log_rets)
    log_rets_df["year"] = log_rets_df.index.year
    log_rets_df["month"] = log_rets_df.index.month
    log_rets_df.reset_index(inplace=True)
    monthly_log_returns = log_rets_df.groupby(["year", "month"])[srs_name].sum()
    monthly_pct_returns = fin.convert_log_rets_to_pct_rets(monthly_log_returns)
    monthly_pct_spread = monthly_pct_returns.unstack()
    monthly_pct_spread.columns = monthly_pct_spread.columns.map(
        lambda x: calendar.month_abbr[x]
    )
    return monthly_pct_spread
