"""
Import as:

import core.model_plotter as modplot
"""

import logging
from typing import Any, List, Optional, Tuple

import matplotlib as mpl
import pandas as pd

import core.plotting as plot
import core.timeseries_study as tss
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class DataFramePlotter:
    """
    Wraps a ModelEvaluator with plotting functionality.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """
        Initialize by supplying a dataframe.

        :param df:
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df

    def plot_autocorrelation(
        self,
        cols: Optional[List[Any]] = None,
        plot_auto_correlation_kwargs: Optional[dict] = None,
    ) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        plot_auto_correlation_kwargs = plot_auto_correlation_kwargs or {}
        plot.plot_autocorrelation(self.df[cols], **plot_auto_correlation_kwargs)

    def plot_sequence_and_density(
        self,
        cols: Optional[List[Any]] = None,
        plot_cols_kwargs: Optional[dict] = None,
    ) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        plot_cols_kwargs = plot_cols_kwargs or {}
        plot.plot_cols(self.df[cols], **plot_cols_kwargs)

    def plot_spectrum(
        self,
        cols: Optional[List[Any]] = None,
        plot_spectrum_kwargs: Optional[dict] = None,
    ) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        plot_spectrum_kwargs = plot_spectrum_kwargs or {}
        plot.plot_spectrum(self.df[cols], **plot_spectrum_kwargs)

    def plot_correlation_matrix(
        self,
        cols: Optional[List[Any]] = None,
        plot_correlation_matrix_kwargs: Optional[dict] = None,
    ) -> None:

        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        plot_correlation_matrix_kwargs = plot_correlation_matrix_kwargs or {}
        plot.plot_correlation_matrix(
            self.df[cols], **plot_correlation_matrix_kwargs
        )

    def plot_dendrogram(
        self,
        cols: Optional[List[Any]] = None,
        figsize: Optional[Tuple[int, int]] = None,
    ) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        plot.plot_dendrogram(self.df[cols], figsize)

    def plot_pca_components(
        self,
        cols: Optional[List[Any]] = None,
        num_components: Optional[int] = None,
    ) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        pca = plot.PCA(mode="standard")
        pca.fit(self.df.fillna(0))
        pca.plot_components(num_components)

    def plot_explained_variance(self, cols: Optional[List[Any]] = None,) -> None:
        cols = cols or self.df.columns
        dbg.dassert_is_subset(cols, self.df.columns)
        pca = plot.PCA(mode="standard")
        pca.fit(self.df.fillna(0))
        pca.plot_explained_variance()

    def plot_qq(
        self,
        col: Any,
        ax: Optional[mpl.axes.Axes] = None,
        dist: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        dbg.dassert_in(col, self.df.columns)
        plot.plot_qq(self.df[col], ax=ax, dist=dist, nan_mode=nan_mode)

    def plot_rolling_correlation(
        self,
        col1: Any,
        col2: Any,
        tau: float,
        plot_rolling_correlation_kwargs: Optional[dict] = None,
    ) -> None:
        dbg.dassert_in(col1, self.df.columns)
        dbg.dassert_in(col2, self.df.columns)
        plot_rolling_correlation_kwargs = plot_rolling_correlation_kwargs or {}
        plot.plot_rolling_correlation(
            self.df[col1],
            self.df[col2],
            tau=tau,
            **plot_rolling_correlation_kwargs,
        )

    def plot_time_series_study(self, col: Any,) -> None:
        dbg.dassert_in(col, self.df.columns)
        tsds = tss.TimeSeriesDailyStudy(self.df[col], data_name=str(col))
        tsds.execute()
