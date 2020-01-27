"""
Import as:

import core.plotting as plot

Utility functions for Jupyter notebook to:
- plot dara

These functions are used for both interactive data exploration and to implement
more complex pipelines.
"""

import logging
import math
from typing import Any, List, Optional, Tuple, Union

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
import sklearn.metrics as skl_metrics

import core.explore as expl
import helpers.dbg as dbg
import helpers.list as hlist

_LOG = logging.getLogger(__name__)

sns.set_palette("bright")

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


def get_heatmap_mask(corr, mode: str) -> np.ndarray:
    if mode == "heatmap_semitriangle":
        # Generate a mask for the upper triangle.
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True
    elif mode == "heatmap":
        mask = None
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    return mask


def get_heatmap_colormap() -> matplotlib.colors.LinearSegmentedColormap:
    """
    Generate a custom diverging colormap useful for heatmaps.
    """
    cmap = sns.diverging_palette(220, 10, as_cmap=True)
    return cmap


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
    cmap = get_heatmap_colormap()
    if figsize is None:
        figsize = FIG_SIZE
    if mode in ("heatmap", "heatmap_semitriangle"):
        # Set up the matplotlib figure.
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        mask = get_heatmap_mask(corr_df, mode)
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
) -> pd.core.frame.DataFrame:
    """
    Compute correlation matrix and plot its heatmap .

    :df: Df to compute correlation matrix and plot a heatmap.
    :mode: "heatmap_semitriangle", "heatmap" or "clustermap".
    :annot:
    :figsize: If nothing specified, basic (20,5) used.
    :title: Title for the plot.
    """
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
    z = scipy.cluster.hierarchy.linkage(y, "average")
    if figsize is None:
        figsize = FIG_SIZE
    _ = plt.figure(figsize=figsize)
    scipy.cluster.hierarchy.dendrogram(
        z,
        labels=df.columns.tolist(),
        leaf_rotation=0,
        color_threshold=0,
        orientation="right",
    )


def display_corr_df(df: pd.core.frame.DataFrame) -> None:
    """
    Display a correlation df with values with 2 decimal places.
    """
    if df is not None:
        df_tmp = df.applymap(lambda x: "%.2f" % x)
        expl.display_df(df_tmp)
    else:
        _LOG.warning("Can't display correlation df since it is None")


def plot_confuision_heatmap(
    y_true: Union[List[Union[float, int]], np.array],
    y_pred: Union[List[Union[float, int]], np.array],
    percentage: bool = False,
) -> None:
    """
    Construct and plot a heatmap for a confusion matrix of fact and prediction.

    :y_true: True values.
    :y_pred: Predictions.
    :percentage: to represent values from confusion matrix in percentage or not.
    """
    confusion = skl_metrics.confusion_matrix(y_true, y_pred)
    labels = set(list(y_true))
    df_cm = pd.DataFrame(confusion, index=labels, columns=labels)
    if percentage:
        df_out = df_cm.apply(lambda x: x / x.sum(), axis=1)
    else:
        df_out = df_cm
    plot_heatmap(df_out, mode="heatmap")


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


def plot_categories_count(
    df: pd.core.frame.DataFrame,
    category_column: str,
    figsize: Optional[Tuple[int, int]] = None,
    title: Optional[str] = None,
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
    num_categories = df[category_column].nunique()
    if num_categories > 10:
        ylen = math.ceil(num_categories / 26) * 5
        figsize = (figsize[0], ylen)
        plt.figure(figsize=figsize)
        ax = sns.countplot(
            y=df[category_column], order=df[category_column].value_counts().index
        )
        ax.set(xlabel="Number of rows")
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
        ax.set(ylabel="Number of rows")
        for p in ax.patches:
            ax.annotate(p.get_height(), (p.get_x() + 0.35, p.get_height() + 1))
    if not title:
        plt.title(f"Distribution by {category_column}")
    else:
        plt.title(title)
    plt.show()
