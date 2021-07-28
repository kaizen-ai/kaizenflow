"""
Implement several data source nodes.

Import as:

import core.dataflow.nodes.sources as cdtfns
"""

import datetime
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

import core.artificial_signal_generators as cartif
import core.dataflow.nodes.base as cdnb
import core.finance as cfinan
import core.pandas_helpers as pdhelp
import helpers.datetime_ as hdatetime
import helpers.hnumpy as hnumpy
import helpers.dbg as dbg
import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)

# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################


# TODO(gp): -> DataSourceFromFixedDf?
class ReadDataFromDf(cdnb.DataSource):
    """
    Data source node accepting data as a DataFrame passed through the constructor.
    """

    def __init__(self, nid: str, df: pd.DataFrame) -> None:
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


# #############################################################################


# TODO(gp): -> DataSourceFromFunction
class DataLoader(cdnb.DataSource):
    """
    Data source node using the passed function and arguments to generate the data.
    """

    def __init__(
        self,
        nid: str,
        func: Callable,
        func_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Constructor.

        :param func: function used to generate data. Typically it accepts
            `start_date` and `end_date` as parameters
        :param func_kwargs: kwargs passed to the function when generating and loading
            the data
        """
        super().__init__(nid)
        self._func = func
        self._func_kwargs = func_kwargs or {}

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load()
        return super().predict()

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        df_out = self._func(**self._func_kwargs)
        # TODO(gp): Add more checks like df.index is an increasing timestamp.
        dbg.dassert_isinstance(df_out, pd.DataFrame)
        self.df = df_out


# #############################################################################


def load_data_from_disk(
    file_path: str,
    # TODO(gp): -> index_col? (Like pandas naming)
    timestamp_col: Optional[str] = None,
    start_date: Optional[_PANDAS_DATE_TYPE] = None,
    end_date: Optional[_PANDAS_DATE_TYPE] = None,
    aws_profile: Optional[str] = None,
    reader_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Read data from CSV or Parquet `file_path`.

    :param timestamp_col: name of the column to use as index
    :param start_date: data to read
    """
    reader_kwargs = reader_kwargs or {}
    kwargs = reader_kwargs.copy()
    # Add S3 credentials, if needed.
    if aws_profile:
        s3fs = hs3.get_s3fs(aws_profile)
        kwargs["s3fs"] = s3fs
    # Get the extension.
    ext = os.path.splitext(file_path)[-1]
    # Select the reading method based on the extension.
    if ext == ".csv":
        # Assume that the first column is the index, unless specified.
        if "index_col" not in reader_kwargs:
            kwargs["index_col"] = 0
        read_data = pdhelp.read_csv
    elif ext == ".pq":
        read_data = pdhelp.read_parquet
    else:
        raise ValueError("Invalid file extension='%s'" % ext)
    # Read the data.
    _LOG.debug("filepath=%s kwargs=%s", file_path, str(kwargs))
    df = read_data(file_path, **kwargs)
    # Process the data.
    # Use the specified timestamp column as index, if needed.
    if timestamp_col is not None:
        df.set_index(timestamp_col, inplace=True)
    # Convert index in timestamps.
    df.index = pd.to_datetime(df.index)
    dbg.dassert_strictly_increasing_index(df)
    # Filter by start / end date.
    # TODO(gp): Not sure that a view is enough to force discarding the unused
    #  rows in the DataFrame. Maybe do a copy, delete the old data, and call the
    #  garbage collector.
    # TODO(gp): A bit inefficient since Parquet might allow to read only the needed
    #  data.
    df = df.loc[start_date:end_date]
    dbg.dassert(not df.empty, "Dataframe is empty")
    return df


class DiskDataSource(cdnb.DataSource):
    """
    Data source node reading CSV or Parquet data from disk.
    """

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
        Constructor.

        :param nid: node identifier
        :param file_path: path to the file to read with ".csv" or ".pq" extension
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
        Load the data on the first invocation and then delegate to the base
        class.

        We don't need to implement `predict()` since the data is read only on the
        first call to `fit()`, so the behavior of the base class is sufficient.

        :return: dict from output name to DataFrame
        """
        self._lazy_load()
        return super().fit()

    def _lazy_load(self) -> None:
        """
        Load the data if it was not already done.
        """
        if self.df is not None:
            return
        df = load_data_from_disk(
            self._file_path,
            self._timestamp_col,
            self._start_date,
            self._end_date,
            self._reader_kwargs,
        )
        self.df = df


# #############################################################################


# TODO(gp): -> ArmaDataSource
class ArmaGenerator(cdnb.DataSource):
    """
    Data source node generating price data from ARMA process returns.
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
        # TODO(gp): -> burn_in_period? Otherwise it seems burning without the final g.
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
        df = prices.to_frame()
        self.df = df.loc[self._start_date : self._end_date]
        # Use constant volume (for now).
        self.df["vol"] = 100


# #############################################################################


# TODO(gp): -> MultivariateNormalDataSource
class MultivariateNormalGenerator(cdnb.DataSource):
    """
    Data source node generating price data from multivariate normal returns.
    """

    def __init__(
        self,
        nid: str,
        frequency: str,
        start_date: _PANDAS_DATE_TYPE,
        end_date: _PANDAS_DATE_TYPE,
        dim: int,
        target_volatility: Optional[float] = None,
        seed: Optional[float] = None,
    ) -> None:
        super().__init__(nid)
        self._frequency = frequency
        self._start_date = start_date
        self._end_date = end_date
        self._dim = dim
        self._target_volatility = target_volatility
        self._volatility_scale_factor = 1
        self._seed = seed
        self._multivariate_normal_process = cartif.MultivariateNormalProcess()
        # Initialize process with appropriate dimension.
        self._multivariate_normal_process.set_cov_from_inv_wishart_draw(
            dim=self._dim, seed=self._seed
        )

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load(fit=True)
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._lazy_load(fit=False)
        return super().predict()

    def _generate_returns(self, fit: bool) -> pd.DataFrame:
        rets = self._multivariate_normal_process.generate_sample(
            date_range_kwargs={
                "start": self._start_date,
                "end": self._end_date,
                "freq": self._frequency,
            },
            seed=self._seed,
        )
        if self._target_volatility is None:
            return rets
        if fit:
            avg_rets = rets.mean(axis=1)
            vol = cfinan.compute_annualized_volatility(avg_rets)
            self._volatility_scale_factor = self._target_volatility / vol
        return rets * self._volatility_scale_factor

    def _lazy_load(self, fit: bool) -> None:
        if self.df is not None:
            return
        rets = self._generate_returns(fit)
        # Cumulatively sum to generate a price series (implicitly assumes the
        # returns are log returns; at small enough scales and short enough
        # times this is practically interchangeable with percentage returns).
        prices = np.exp(rets.cumsum())
        prices = prices.rename(columns=lambda x: "MN" + str(x))
        # Use constant volume (for now).
        volume = pd.DataFrame(
            index=prices.index, columns=prices.columns, data=100
        )
        df = pd.concat([prices, volume], axis=1, keys=["close", "volume"])
        self.df = df.loc[self._start_date : self._end_date]


# #############################################################################

# TODO(gp): Create a RealTimeMixin to inject the RT behavior for all the nodes.
# TODO(gp): Use the new approach of DataLoader, moving the function out.

class RealTimeSyntheticDataSource(cdnb.DataSource):
    """
    Data source node that outputs data mimicking the real-time behavior.

    Depending on the current wall-clock time (which is set through `set_not_time()`)
    this node emits the data available up and including that time.
    """

    def __init__(
        self,
        nid: str,
        columns: List[str],
        start_date: Optional[hdatetime.Datetime] = None,
        end_date: Optional[hdatetime.Datetime] = None,
        freq: str = "1T",
        seed: int = 42,
    ) -> None:
        """
        Constructor.
        
        :param columns: list of columns to generate
        :param freq: Pandas freq (e.g., `1T` for 1 minute) used to iterate between
            [start_date, end_date]
        """
        super().__init__(nid)
        dbg.dassert_container_type(columns, list, str)
        self._columns = columns
        self._start_date = start_date
        self._end_date = end_date
        dbg.dassert_isinstance(freq, str)
        self._freq = freq
        dbg.dassert_isinstance(seed, int)
        self._seed = seed
        # This indicates what is the current time is, so that the node can emit data
        # up to that time.
        self._current_time : Optional[pd.Timestamp] = None
        # Store the entire history of the data.
        self._entire_df : Optional[pd.DataFrame] = None

    def set_current_time(self, datetime_: pd.Timestamp) -> None:
        """
        Set the current simulation time.
        """
        dbg.dassert_isinstance(datetime_, pd.Timestamp)
        # Time only moves forward.
        if self._current_time is not None:
            dbg.dassert_lte(self._current_time, datetime_)
        self._current_time = datetime_

    def reset_current_time(self) -> None:
        """
        Reset the current simulation time to `None`.
        """
        self._current_time = None

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._get_data_until_current_time()
        return super().fit()

    def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
        self._get_data_until_current_time()
        return super().predict()

    def _get_data_until_current_time(self) -> None:
        """
        Set the data stored inside the node up and including `self._current_time`.

        E.g., if `self._current_time = pd.Timestamp("2010-01-04 09:34:00"`)`,
        then `self.df` looks like:
        ```
                                close    volume
        ...                       ...       ...
        2010-01-04 09:31:00 -0.377824 -0.660321
        2010-01-04 09:32:00 -0.508435 -0.349565
        2010-01-04 09:33:00 -0.151361  0.139516
        2010-01-04 09:34:00  0.046069 -0.040318
        ```
        """
        dbg.dassert_is_not(self._current_time, None,
                           "The current time needs to be set with `set_current_time()`")
        self._lazy_load()
        self.df = self._entire_df.loc[:self._current_time]

    def _lazy_load(self) -> None:
        if self._entire_df is not None:
            return
        dates = pd.date_range(self._start_date, self._end_date, freq="1T")
        # Random walk with increments independent and uniform in [-0.5, 0.5].
        with hnumpy.random_seed_context(self._seed):
            data = np.random.rand(len(dates), len(self._columns)) - 0.5
        df = pd.DataFrame(data, columns=self._columns, index=dates)
        df = df.cumsum()
        self._entire_df = df