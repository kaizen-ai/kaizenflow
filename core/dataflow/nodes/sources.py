import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

import core.artificial_signal_generators as cartif
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

from core.dataflow.nodes.base import (
    DataSource
)

# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################
# Data source nodes
# #############################################################################


class ReadDataFromDf(DataSource):
    def __init__(self, nid: str, df: pd.DataFrame) -> None:
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


class DiskDataSource(DataSource):
    def __init__(
            self,
            nid: str,
            file_path: str,
            timestamp_col: Optional[str] = None,
            start_date: Optional[_PANDAS_DATE_TYPE] = None,
            end_date: Optional[_PANDAS_DATE_TYPE] = None,
            reader_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create data source node reading CSV or parquet data from disk.

        :param nid: node identifier
        :param file_path: path to the file
        # TODO(*): Don't the readers support this already?
        :param timestamp_col: name of the timestamp column. If `None`, assume
            that index contains timestamps
        :param start_date: data start date in timezone of the dataset, included
        :param end_date: data end date in timezone of the dataset, included
        :param reader_kwargs: kwargs for the data reading function
        """
        super().__init__(nid)
        self._file_path = file_path
        self._timestamp_col = timestamp_col
        self._start_date = start_date
        self._end_date = end_date
        self._reader_kwargs = reader_kwargs or {}

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()

    def _read_data(self) -> None:
        ext = os.path.splitext(self._file_path)[-1]
        if ext == ".csv":
            if "index_col" not in self._reader_kwargs:
                self._reader_kwargs["index_col"] = 0
            read_data = pd.read_csv
        elif ext == ".pq":
            read_data = pd.read_parquet
        else:
            raise ValueError("Invalid file extension='%s'" % ext)
        self.df = read_data(self._file_path, **self._reader_kwargs)

    def _process_data(self) -> None:
        if self._timestamp_col is not None:
            self.df.set_index(self._timestamp_col, inplace=True)
        self.df.index = pd.to_datetime(self.df.index)
        dbg.dassert_strictly_increasing_index(self.df)
        self.df = self.df.loc[self._start_date : self._end_date]
        dbg.dassert(not self.df.empty, "Dataframe is empty")

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        self._read_data()
        self._process_data()


class ArmaGenerator(DataSource):
    """
    A node for generating price data from ARMA process returns.
    """

    def __init__(
            self,
            nid: str,
            frequency: str,
            start_date: _PANDAS_DATE_TYPE,
            end_date: _PANDAS_DATE_TYPE,
            ar_coeffs: Optional[List[float]] = None,
            ma_coeffs: Optional[List[float]] = None,
            scale: Optional[float] = None,
            burnin: Optional[float] = None,
            seed: Optional[float] = None,
    ) -> None:
        super().__init__(nid)
        self._frequency = frequency
        self._start_date = start_date
        self._end_date = end_date
        self._ar_coeffs = ar_coeffs or [0]
        self._ma_coeffs = ma_coeffs or [0]
        self._scale = scale or 1
        self._burnin = burnin or 0
        self._seed = seed
        self._arma_process = cartif.ArmaProcess(
            ar_coeffs=self._ar_coeffs, ma_coeffs=self._ma_coeffs
        )

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        rets = self._arma_process.generate_sample(
            date_range_kwargs={
                "start": self._start_date,
                "end": self._end_date,
                "freq": self._frequency,
            },
            scale=self._scale,
            burnin=self._burnin,
            seed=self._seed,
        )
        # Cumulatively sum to generate a price series (implicitly assumes the
        # returns are log returns; at small enough scales and short enough
        # times this is practically interchangeable with percentage returns).
        # TODO(*): Allow specification of annualized target volatility.
        prices = np.exp(0.1 * rets.cumsum())
        prices.name = "close"
        self.df = prices.to_frame()
        self.df = self.df.loc[self._start_date : self._end_date]
        # Use constant volume (for now).
        self.df["vol"] = 100


class MultivariateNormalGenerator(DataSource):
    """
    A node for generating price data from multivariate normal returns.
    """

    def __init__(
            self,
            nid: str,
            frequency: str,
            start_date: _PANDAS_DATE_TYPE,
            end_date: _PANDAS_DATE_TYPE,
            dim: int,
            seed: Optional[float] = None,
    ) -> None:
        super().__init__(nid)
        self._frequency = frequency
        self._start_date = start_date
        self._end_date = end_date
        self._dim = dim
        self._seed = seed
        self._multivariate_normal_process = cartif.MultivariateNormalProcess()
        # Initialize process with appropriate dimension.
        self._multivariate_normal_process.set_cov_from_inv_wishart_draw(
            dim=self._dim, seed=self._seed
        )

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        rets = self._multivariate_normal_process.generate_sample(
            date_range_kwargs={
                "start": self._start_date,
                "end": self._end_date,
                "freq": self._frequency,
            },
            seed=self._seed,
        )
        # Cumulatively sum to generate a price series (implicitly assumes the
        # returns are log returns; at small enough scales and short enough
        # times this is practically interchangeable with percentage returns).
        # TODO(*): We hard-code a scale factor to make these look more
        #     realistic, but it would be better to allow the user to specify
        #     a target annualized volatility.
        prices = np.exp(0.1 * rets.cumsum())
        prices = prices.rename(columns=lambda x: "MN" + str(x))
        # Use constant volume (for now).
        volume = pd.DataFrame(
            index=prices.index, columns=prices.columns, data=100
        )
        df = pd.concat([prices, volume], axis=1, keys=["close", "vol"])
        self.df = df
        self.df = self.df.loc[self._start_date : self._end_date]


