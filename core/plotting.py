"""Import as:

import core.plotting as plot
"""

import calendar
import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Union

import core.explore as expl
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dataframe as hdf
import helpers.dbg as dbg
import helpers.list as hlist
import helpers.printing as prnt
import matplotlib as mpl
import matplotlib.cm as cm
import matplotlib.colors as mpl_col
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy as sp
import scipy.cluster.hierarchy as hac
import seaborn as sns
import sklearn.decomposition as skldec
import sklearn.metrics as sklmet
import sklearn.utils.validation as skluv
import statsmodels.api as sm
import statsmodels.regression.rolling as smrr

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
    """Plot a diagram describing the non-nans intervals for the columns of df.

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
    """Plot countplot of a given `category_column`.

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
    """Create figure to accommodate `num_plots` plots. The figure is arranged
    in rows with `num_cols` columns.

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
    """Plot barplots for the counts of a series and print the values.

    Same interface as plot_count_series() but computing the count of the
    given series `srs`.
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
    """Plot barplots for series containing counts and print the values.

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
    top_n_to_plot: Optional[int] = None,
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    unicolor: bool = False,
    color_palette: Optional[List[Tuple[float, float, float]]] = None,
    figsize: Optional[Tuple[int, int]] = None,
    rotation: int = 0,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """Plot a barplot.

    :param srs: pd.Series
    :param orientation: vertical or horizontal bars
    :param annotation_mode: `pct`, `value` or None
    :param string_format: format of bar annotations
    :param top_n_to_plot: number of top N integers to plot
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

    # Get default figure size.
    if figsize is None:
        figsize = FIG_SIZE
    if top_n_to_plot is None:
        # If top_n not specified, plot all values.
        srs_top_n = srs
    else:
        # Assert N>0.
        dbg.dassert_lte(1, top_n_to_plot)
        # Sort in descending order.
        srs_sorted = srs.sort_values(ascending=False)
        # Select top N.
        srs_top_n = srs_sorted[:top_n_to_plot]
    # Choose colors.
    if unicolor:
        color = sns.color_palette("muted")[0]
    else:
        color_palette = color_palette or sns.diverging_palette(10, 133, n=2)
        color = (srs > 0).map({True: color_palette[-1], False: color_palette[0]})
    # Choose orientation.
    if orientation == "vertical":
        kind = "bar"
    elif orientation == "horizontal":
        kind = "barh"
    else:
        raise ValueError("Invalid orientation='%s'" % orientation)
    ax = ax or plt.gca()
    # Plot top N.
    srs_top_n.plot(
        kind=kind, color=color, rot=rotation, title=title, ax=ax, figsize=figsize
    )
    # Add annotations to bars.
    # Note: annotations in both modes are taken from
    # entire series, not top N.
    if annotation_mode:
        if annotation_mode == "pct":
            annotations = srs * 100 / srs.sum()
            string_format = string_format + "%%"
            annotations = annotations.apply(lambda z: string_format % z)
        elif annotation_mode == "value":
            annotations = srs.apply(lambda z: string_format % z)
        else:
            raise ValueError("Invalid annotations_mode='%s'" % annotation_mode)
        # Annotate bars.
        for i, p in enumerate(ax.patches):
            height = p.get_height()
            width = p.get_width()
            x, y = p.get_xy()
            annotation_loc = _get_annotation_loc(x, y, height, width)
            ax.annotate(annotations.iloc[i], annotation_loc)
    # Set X-axis label.
    if xlabel:
        ax.set(xlabel=xlabel)


# #############################################################################
# Time series plotting
# #############################################################################


def plot_timeseries_distribution(
    srs: pd.Series,
    datetime_types: Optional[List[str]] = None,
) -> None:
    """Plot timeseries distribution by.

    - "year",
    - "month",
    - "quarter",
    - "weekofyear",
    - "dayofweek",
    - "hour",
    - "second"
    unless otherwise provided by `datetime_types`.

    :param srs: timeseries pd.Series to plot
    :param datetime_types: types of pd.datetime, e.g. "month", "quarter"
    """
    dbg.dassert_isinstance(srs, pd.Series)
    dbg.dassert_isinstance(srs.index, pd.DatetimeIndex)
    srs = hdf.apply_nan_mode(srs, mode="drop")
    index_series = pd.Series(srs.index)
    if datetime_types is None:
        datetime_types = _DATETIME_TYPES
    for datetime_type in datetime_types:
        plt.figure(figsize=FIG_SIZE)
        sns.countplot(getattr(index_series.dt, datetime_type))
        plt.title(f"Distribution by {datetime_type}")
        plt.xlabel(datetime_type, fontsize=12)
        plt.ylabel(f"Quantity of {srs.name}", fontsize=12)
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
    """Plot distribution (where `datetime_types` has the same meaning as in
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


# TODO(*): Rename. Maybe `plot_sequence_and_density()`.
def plot_cols(
    data: Union[pd.Series, pd.DataFrame],
    colormap: str = "rainbow",
    mode: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = None,
    figsize: Optional[Tuple[float, float]] = (20, 10),
) -> None:
    """Plot lineplot and density plot for the given dataframe.

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
    data.replace([np.inf, -np.inf], np.nan).plot(
        kind="density", colormap=colormap, ax=axes[0]
    )
    data.plot(colormap=colormap, ax=axes[1])


# TODO(*): Check that data index size exceeds lags.
def plot_autocorrelation(
    signal: Union[pd.Series, pd.DataFrame],
    lags: int = 40,
    zero: bool = False,
    nan_mode: str = "conservative",
    fft: bool = True,
    title_prefix: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = None,
    **kwargs: Any,
) -> None:
    """Plot ACF and PACF of columns.

    https://www.statsmodels.org/stable/_modules/statsmodels/graphics/tsaplots.html#plot_acf
    https://www.statsmodels.org/stable/_modules/statsmodels/tsa/stattools.html#acf
    """
    if axes is None:
        axes = [[None, None]]
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
            data,
            lags=lags,
            ax=ax2,
            zero=zero,
            title=pacf_title,
            **kwargs,
        )


def plot_spectrum(
    signal: Union[pd.Series, pd.DataFrame],
    nan_mode: str = "conservative",
    title_prefix: Optional[str] = None,
    axes: Optional[List[mpl.axes.Axes]] = None,
) -> None:
    """Plot power spectral density and spectrogram of columns.

    PSD:
      - Estimate the power spectral density using Welch's method.
      - Related to autocorrelation via the Fourier transform (Wiener-Khinchin).
    Spectrogram:
      - From the scipy documentation of spectrogram:
        "Spectrograms can be used as a way of visualizing the change of a
         nonstationary signal's frequency content over time."
    """
    if axes is None:
        axes = [[None, None]]
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


def plot_time_series_dict(
    dict_: Dict[str, pd.Series],
    num_plots: Optional[int] = None,
    num_cols: Optional[int] = 2,
    y_scale: Optional[float] = 4,
    sharex: bool = True,
    sharey: bool = False,
    exclude_empty: bool = True,
) -> None:
    """
    Plot series from a dict of series.

    :param dict_: dict of series
    :param num_plots: number of plots
    :param num_cols: number of columns to use in the subplot
    :param y_scale: scale of y-axis
    :param sharex: unify x-axis if True
    :param sharey: unify y-axis if True
    :param exclude_empty: whether to exclude plots of empty series
    """
    if exclude_empty:
        non_empty_dict_ = {
            key: val for key, val in dict_.items() if not val.empty
        }
        if len(non_empty_dict_) < len(dict_):
            excluded_series = set(dict_).difference(non_empty_dict_)
            _LOG.warning("Excluded empty series: %s", excluded_series)
        dict_ = non_empty_dict_
    num_plots = num_plots or len(dict_)
    # Create figure to accommodate plots.
    _, axes = get_multiple_plots(
        num_plots=num_plots,
        num_cols=num_cols,
        y_scale=y_scale,
        sharex=sharex,
        sharey=sharey,
    )
    # Select first `num_plots` series in the dict and plot them.
    keys_to_draw = list(dict_.keys())[:num_plots]
    for i, key in enumerate(keys_to_draw):
        srs = dict_[key]
        srs.to_frame().plot(title=key, ax=axes[i])


# #############################################################################
# Correlation-type plots
# #############################################################################


def plot_heatmap(
    corr_df: pd.core.frame.DataFrame,
    mode: Optional[str] = None,
    annot: Union[bool, str] = "auto",
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
    vmin: float = -1.0,
    vmax: float = 1.0,
    ax: Optional[plt.axes] = None,
) -> None:
    """Plot a heatmap for a corr / cov df.

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
    if corr_df.empty:
        _LOG.warning("Can't plot heatmap for empty `corr_df`")
        return
    if corr_df.shape[0] > 20:
        _LOG.warning("The corr_df.shape[0]='%s' > 20", corr_df.shape[0])
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
    mode = mode or "heatmap"
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
    mode: Optional[str] = None,
    annot: Union[bool, str] = False,
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
    method: Optional[str] = None,
    min_periods: Optional[int] = None,
) -> pd.core.frame.DataFrame:
    """Compute correlation matrix and plot its heatmap.

    :param df: Df to compute correlation matrix and plot a heatmap
    :param mode: "heatmap_semitriangle", "heatmap" or "clustermap"
    :param annot: determines whether to use annotations
    :param figsize: if nothing specified, basic (20,5) used
    :param title: title for the plot
    :param method: "pearson", "kendall", "spearman" or callable method of correlation
    :param min_periods: minimum number of observations required per pair of columns to have
        a valid result; currently only available for Pearson and Spearman correlation
    """
    if df.empty:
        _LOG.warning("Skipping correlation matrix since `df` is empty")
        return None
    # Compute the correlation matrix.
    method = method or "pearson"
    corr_df = df.corr(method=method, min_periods=min_periods)
    # Plot heatmap.
    plot_heatmap(
        corr_df,
        mode=mode,
        annot=annot,
        figsize=figsize,
        title=title,
        vmin=-1.0,
        vmax=1.0,
    )
    return corr_df


def display_corr_df(df: pd.core.frame.DataFrame) -> None:
    """Display a correlation df with values with 2 decimal places."""
    if df is not None:
        df_tmp = df.applymap(lambda x: "%.2f" % x)
        expl.display_df(df_tmp)
    else:
        _LOG.warning("Can't display correlation df since it is None")

def compute_linkage(df: pd.DataFrame) -> np.ndarray:
    return hac.linkage(df, method="average")

def cluster_and_select(
    df: pd.DataFrame,
    num_clust: int,
    corr_thr: float = 0.8,
    show_corr_plots: bool = True,
    show_dendogram: bool = True,
    z_linkage: Optional[np.ndarray] = None,
) -> List:
    """
    Function to cluster time series and select least correlated ones in each cluster.
    
    :param df: df input dataframe 
    :param num_clust: int number of clusters to compute
    :param corr_thr: float correlation threshold
    :param show_corr_plots: bool whether to show correlation plots or not
    :param show_dendogram: bool whether to show original clustering dendogram 
    :param z_linkage: ndarray optional pre-computed cluster array
    :returns: list of names of series to keep
    :rtypes: List
    """
    df = df.drop(df.columns[df.nunique() == 1], axis=1)
    if df.shape[1] < 2:
        _LOG.warning("Skipping correlation matrix since df is %s", str(df.shape))
        return
    # Cluster the time series.
    corr = df.corr()
    if z_linkage is None:
        z_linkage = compute_linkage(corr)
    clusters = hac.fcluster(z_linkage, num_clust, criterion="maxclust")
    series_to_keep = []
    df_name_clust = pd.DataFrame(
        {"name": list(df.columns.values), "cluster": clusters}
    )
    # Plot the dendogram of clustered series.
    if show_dendogram:
        plot_dendrogram(df, z_linkage)
    # Plot the correlation heatmap for each cluster and drop highly correlated ts.
    for i in range(1, num_clust + 1):
        print(prnt.frame(f"Cluster {i}"))
        names = list(set(df_name_clust[df_name_clust.cluster == i].name.values))
        cluster_subset = df[names]
        cluster_corr = cluster_subset.corr().abs()
        if show_corr_plots:
            sns.heatmap(cluster_corr, cmap="RdBu_r", vmin=0, vmax=1)
            plt.show()
        remaining = list(names.copy())
        # Remove series that have correlation above the threshold specified.
        for j in range(0, len(names)):
            for k in range(j + 1, len(names) - 1):
                corr_series = cluster_corr.loc[names[j]].loc[names[k]]
                if corr_series >= corr_thr:
                    try:
                        remaining.remove(names[j])
                    except:
                        ValueError
        series_to_keep = series_to_keep + list(set(remaining))
        plt.show()
        print(list(set(remaining)))
        print(
            prnt.frame(
                f"Number of original series in cluser {i} is {len(set(names))}"
                + "\n"
                + f"Number of series to keep in cluster {i} is {len(set(remaining))}"
            )
        )
    # Print the final list of series to keep.
    print(" ")
    print(
        prnt.frame(
            f"Final number of selected time series is {len(series_to_keep)}"
        )
    )
    print(prnt.frame(f"Series to keep are: {series_to_keep}"))
    return series_to_keep


def plot_dendrogram(
    df: pd.core.frame.DataFrame,
    z: Optional[np.ndarray] = None,
    figsize: Optional[Tuple[int, int]] = None,
    **kwargs: Any,
) -> None:
    """Plot a dendrogram.

    A dendrogram is a diagram representing a tree.

    :param df: df to plot a heatmap
    :param z: precomputed cluster array
    :param figsize: if nothing specified, basic (20,5) used
    :param kwargs: kwargs for `sp.cluster.hierarchy.dendrogram`
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
    if z is None:
        z = sp.cluster.hierarchy.linkage(y, "average")
    else:
        pass
    if figsize is None:
        figsize = FIG_SIZE
    _ = plt.figure(figsize=figsize)
    plt.title("Hierarchical Clustering Dendrogram")
    plt.ylabel("Distance")
    sp.cluster.hierarchy.dendrogram(
        z,
        labels=df.columns.tolist(),
        **kwargs,
    )


def plot_corr_over_time(
    corr_df: pd.core.frame.DataFrame,
    mode: Optional[str] = None,
    annot: bool = False,
    num_cols: int = 4,
) -> None:
    """Plot correlation over time."""
    mode = mode or "heatmap"
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
        """Plot principal components.

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
        """Get the number of principal components to plot."""
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
    """Generate a custom diverging colormap useful for heatmaps."""
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
    """Construct and plot a heatmap for a confusion matrix of fact and
    prediction.

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
    return None


def multipletests_plot(
    pvals: pd.Series,
    threshold: float,
    adj_pvals: Optional[Union[pd.Series, pd.DataFrame]] = None,
    num_cols: Optional[int] = None,
    method: Optional[str] = None,
    suptitle: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Plot adjusted p-values and pass/fail threshold.

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
        ax[i].axhline(threshold, ls="--", c="k")
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
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot cumulative returns.

    :param cumulative_rets: log or pct cumulative returns
    :param mode: log or pct, used to choose plot title
    :param unit: "ratio", "%" or "bps", both input series are rescaled
        appropriately
    :param benchmark_series: additional series to plot
    :param title_suffix: suffix added to the title
    :param ax: axes
    :param plot_zero_line: whether to plot horizontal line at 0
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
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
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot rolling annualized volatility.

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
    :param events: list of tuples with dates and labels to point out on the plot
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
        annualized_rolling_volatility,
        label="annualized rolling volatility",
    )
    ax.axhline(
        annualized_volatility,
        linestyle="--",
        linewidth=2,
        color="green",
        label="average annualized volatility",
    )
    ax.axhline(0, linewidth=0.8, color="black")
    _maybe_add_events(ax=ax, events=events)
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
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot rolling annualized Sharpe ratio.

    :param srs: input series
    :param tau: argument as for sigp.compute_smooth_moving_average
    :param min_depth: argument as for sigp.compute_smooth_moving_average
    :param max_depth: argument as for sigp.compute_smooth_moving_average
    :param p_moment: argument as for sigp.compute_smooth_moving_average
    :param ci: confidence interval
    :param title_suffix: suffix added to the title
    :param trim_index: start plot at original index if True
    :param ax: axes
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
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
    """Plot a barplot of log returns statistics by year.

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
    """Plot a heatmap of log returns statistics by year and month.

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
    """Plot pnls for dict of pnl time series.

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
            ", ".join([str(x) for x in empty_srs]),
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
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot drawdown.

    :param log_rets: log returns
    :param unit: `ratio`, `%`, input series is rescaled appropriately
    :param title_suffix: suffix added to the title
    :param ax: axes
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
    ax.set_ylim(top=0)
    ax.set_ylabel(unit)
    ax.legend()


def plot_holdings(
    holdings: pd.Series,
    unit: str = "ratio",
    ax: Optional[mpl.axes.Axes] = None,
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot holdings, average holdings and average holdings by month.

    :param holdings: pnl series to plot
    :param unit: "ratio", "%" or "bps" scaling coefficient
    :param ax: axes in which to draw the plot
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
    ax.set_ylabel(unit)
    ax.legend()
    ax.set_title(f"Total holdings ({unit})")


def plot_qq(
    srs: pd.Series,
    ax: Optional[mpl.axes.Axes] = None,
    dist: Optional[str] = None,
    nan_mode: Optional[str] = None,
) -> None:
    """Plot ordered values against theoretical quantiles of the given
    distribution.

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
    positions: pd.Series,
    unit: str = "ratio",
    ax: Optional[mpl.axes.Axes] = None,
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot turnover, average turnover by month and overall average turnover.

    :param positions: series of positions to plot
    :param unit: "ratio", "%" or "bps" scaling coefficient
    :param ax: axes in which to draw the plot
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
    ax.set_ylabel(unit)
    ax.legend()
    ax.set_title(f"Turnover ({unit})")


def plot_allocation(
    position_df: pd.DataFrame,
    config: Dict[str, Any],
    figsize: Optional[Tuple[int, int]] = None,
    ax: Optional[mpl.axes.Axes] = None,
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """Plot position allocations over time.

    :param position_df: dataframe with position time series
    :param config: information about time series
    :param figsize: size of plot
    :param ax: axes
    :param events: list of tuples with dates and labels to point out on the plot
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
    _maybe_add_events(ax=ax, events=events)
    ax.set_title(
        f"Portfolio allocation over time; {position_df.shape[1]} positions"
    )
    ax.set_xlabel("period")
    ax.legend()


def plot_rolling_beta(
    rets: pd.Series,
    benchmark_rets: pd.Series,
    window: int,
    nan_mode: Optional[str] = None,
    ax: Optional[mpl.axes.Axes] = None,
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
    **kwargs: Any,
) -> None:
    """Regress returns against benchmark series and plot rolling beta.

    :param rets: returns
    :param benchmark_rets: benchmark returns
    :param window: length of the rolling window
    :param nan_mode: argument for hdf.apply_nan_mode()
    :param ax: axis
    :param events: list of tuples with dates and labels to point out on the plot
    :param kwargs: kwargs for statsmodels.regression.rolling.RollingOLS
    """
    dbg.dassert_strictly_increasing_index(rets)
    dbg.dassert_strictly_increasing_index(benchmark_rets)
    dbg.dassert_eq(rets.index.freq, benchmark_rets.index.freq)
    # Assert that the 'rets' index is a subset of the 'benchmark_rets' index.
    dbg.dassert(rets.index.isin(benchmark_rets.index).all())
    dbg.dassert_lte(
        window,
        min(len(rets), len(benchmark_rets)),
        "`window` should not be larger than inputs' lengths.",
    )
    rets_name = rets.name
    benchmark_name = benchmark_rets.name
    dbg.dassert_ne(
        rets_name, benchmark_name, "Inputs should have different names."
    )
    nan_mode = nan_mode or "drop"
    # Combine rets and benchmark_rets in one dataframe over the intersection
    #    of their indices.
    all_rets_df = pd.concat([rets, benchmark_rets], axis=1, join="inner")
    all_rets_df.columns = [rets_name, benchmark_name]
    # Extract common index in order to keep NaN periods on the X-axis.
    common_index = all_rets_df.index
    # Apply `.dropna()` after `hdf.apply_nan_mode` in oder to drop remaining
    #     rows with NaNs and calculate rolling beta without NaN gaps in input.
    clean_rets_df = all_rets_df.apply(hdf.apply_nan_mode, mode=nan_mode).dropna()
    # Get copies of rets and benchmark_rets with unified indices and no NaNs.
    rets = clean_rets_df[rets_name]
    benchmark_rets = clean_rets_df[benchmark_name]
    # Calculate and plot rolling beta.
    ax = ax or plt.gca()
    benchmark_rets = sm.add_constant(benchmark_rets)
    # Calculate and plot rolling beta.
    model_rolling = smrr.RollingOLS(rets, benchmark_rets, window=window, **kwargs)
    res_rolling = model_rolling.fit()
    beta_rolling = res_rolling.params[
        benchmark_name
    ]  # pylint: disable=unsubscriptable-object
    # Return NaN periods to the rolling beta series for the plot.
    beta_rolling = beta_rolling.reindex(common_index)
    beta_rolling.plot(
        ax=ax,
        title=f"Beta with respect to {benchmark_name}",
        label="Rolling beta",
    )
    # Calculate and plot beta for the whole period.
    model_whole_period = sm.OLS(rets, benchmark_rets)
    res_whole_period = model_whole_period.fit()
    beta_whole_period = res_whole_period.params[benchmark_name]
    ax.axhline(beta_whole_period, ls="--", c="k", label="Whole-period beta")
    ax.set_xlabel("period")
    ax.set_ylabel("beta")
    _maybe_add_events(ax=ax, events=events)
    ax.legend()


def plot_rolling_correlation(
    srs1: pd.Series,
    srs2: pd.Series,
    tau: float,
    demean: bool = True,
    min_periods: int = 0,
    min_depth: int = 1,
    max_depth: int = 1,
    p_moment: float = 2,
    mode: Optional[str] = None,
    ax: Optional[mpl.axes.Axes] = None,
    events: Optional[List[Tuple[str, Optional[str]]]] = None,
) -> None:
    """
    Return rolling correlation between 2 series and plot rolling correlation.

    :param srs1: first series
    :param srs2: second series
    :param tau: tau correlation coefficient
    :param demean: bool demean
    :param min_periods: min periods
    :param min_depth: min depth
    :param max_depth: max depth
    :param p_moment: p moment
    :param mode: corr or zcorr
    :param ax: axis
    :param events: list of tuples with dates and labels to point out on the plot
    """
    mode = mode or "corr"
    # Calculate and plot rolling correlation.
    ax = ax or plt.gca()
    # Calculate rolling correlation.
    if mode == "zcorr":
        roll_correlation = sigp.compute_rolling_zcorr
        title = "Z Correlation of 2 time series"
        label = "Rolling z correlation"
    elif mode == "corr":
        roll_correlation = sigp.compute_rolling_corr
        title = "Correlation of 2 time series"
        label = "Rolling correlation"
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    # Calculate rolling correlation with the given mode.
    roll_corr = roll_correlation(
        srs1,
        srs2,
        tau=tau,
        demean=demean,
        min_periods=min_periods,
        min_depth=min_depth,
        max_depth=max_depth,
        p_moment=p_moment,
    )
    # Plot rolling correlation.
    roll_corr.plot(ax=ax, title=title, label=label)
    # Calculate correlation whole period.
    whole_period = srs1.corr(srs2)
    # Plot correlation whole period.
    ax.axhline(whole_period, ls="--", c="k", label="Whole-period correlation")
    ax.set_xlabel("period")
    ax.set_ylabel("correlation")
    _maybe_add_events(ax=ax, events=events)
    ax.legend()


def plot_sharpe_ratio_panel(
    log_rets: pd.Series,
    frequencies: Optional[List[str]] = None,
    ax: Optional[mpl.axes.Axes] = None,
) -> None:
    """Plot how SRs vary under resampling.

    :param log_rets: log returns
    :param frequencies: frequencies to calculate SR for
    :param ax: axis
    """
    dbg.dassert_isinstance(log_rets, pd.Series)
    frequencies = frequencies or ["B", "D", "W", "M", "Q"]
    srs_freq = pd.infer_freq(log_rets.index)
    if not srs_freq:
        _LOG.warning("Input has no frequency and it has been rescaled to 'D'")
        srs_freq = "D"
    # Resample input for assuring input frequency in calculations.
    log_rets = sigp.resample(log_rets, rule=srs_freq).sum()
    # Initiate series for Sharpe ratios of selected frequencies.
    sr_series = pd.Series([], dtype="object")
    # Initiate list for Sharpe ratios' standard errors for error bars.
    res_se = []
    # Initiate list for frequencies that do not lead to upsampling.
    valid_frequencies = []
    # Compute input frequency points per year for identifying upsampling.
    input_freq_points_per_year = hdf.infer_sampling_points_per_year(log_rets)
    for freq in frequencies:
        freq_points_per_year = hdf.compute_points_per_year_for_given_freq(freq)
        if freq_points_per_year > input_freq_points_per_year:
            _LOG.warning(
                "Upsampling from input freq='%s' to freq='%s' is blocked",
                srs_freq,
                freq,
            )
            continue
        resampled_log_rets = sigp.resample(log_rets, rule=freq).sum()
        if len(resampled_log_rets) == 1:
            _LOG.warning(
                "Resampling to freq='%s' is blocked because resampled series has only 1 observation",
                freq,
            )
            continue
        sr = stats.compute_annualized_sharpe_ratio(resampled_log_rets)
        se = stats.compute_annualized_sharpe_ratio_standard_error(
            resampled_log_rets
        )
        sr_series[freq] = sr
        res_se.append(se)
        valid_frequencies.append(freq)
    ax = ax or plt.gca()
    sr_series.plot(
        yerr=res_se, marker="o", capsize=2, ax=ax, label="Sharpe ratio"
    )
    ax.set_xticks(range(len(valid_frequencies)))
    ax.set_xticklabels(valid_frequencies)
    ax.set_xlabel("Frequencies")
    ax.legend()


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
    """Calculate log returns statistics by year and month.

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


def _maybe_add_events(
    ax: mpl.axes.Axes, events: Optional[List[Tuple[str, Optional[str]]]]
) -> None:
    """Add labeled vertical lines at events' dates on a plot.

    :param ax: axes
    :param events: list of tuples with dates and labels to point out on the plot
    """
    if not events:
        return None
    colors = cm.get_cmap("Set1")(np.linspace(0, 1, len(events)))
    for event, color in zip(events, colors):
        ax.axvline(
            x=pd.Timestamp(event[0]),
            label=event[1],
            color=color,
            linestyle="--",
        )
    return None
