"""
Import as:

import market_data.abstract_market_data as mdabmada
"""

import abc
import asyncio
import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


_LOG.verb_debug = hprint.install_log_verb_debug(_LOG, verbose=False)


# #############################################################################


AssetId = int


def dassert_valid_asset_ids(asset_ids: Optional[Iterable[AssetId]]) -> None:
    if asset_ids is not None:
        hdbg.dassert_container_type(
            asset_ids, (np.ndarray, list), (int, np.int64)
        )


# #############################################################################
# MarketData
# #############################################################################


class MarketData(abc.ABC):
    """
    Implement an interface to an historical / real-time source of price data.

    `market_data/data_pipeline_architecture.md` discusses the design principles.

    # Responsibilities:
    - Delegate to a data backend in `ImClient` to retrieve historical and real-time
      data
    - Model data in terms of interval `start_timestamp`, `end_timestamp`
        - `ImClient` models data in terms of end timestamp of the interval
    - Implement RT behaviors (e.g, `is_last_bar_available`, wall_clock, ...)
    - Implement knowledge time and delay
        - `ImClient` doesn't have this view of the data
    - Stitch together different data representations (e.g., historical / RT)
      using multiple IM backends
    - Remap columns to connect data backends to consumers
    - Implement some common market-related data transformations
        - E.g., `get_twap_price()`, `get_last_price()`
    - Handle timezones, i.e. convert all timestamp to the provided timezone

    # Non-responsibilities:
    - In general do not access data directly but rely on `ImClient` objects to
      retrieve the data from different backends

    # Output format
    - The class normalizes the data by:
        - sorting by the columns that correspond to `end_time` and `asset_id`
        - indexing by the column that corresponds to `end_time`, so that it is suitable
          to DataFlow computation
    - E.g.,
    ```
                            asset_id                start_time    close   volume
    end_time
    2021-07-20 09:31:00-04:00  17085 2021-07-20 09:30:00-04:00  143.990  1524506
    2021-07-20 09:32:00-04:00  17085 2021-07-20 09:31:00-04:00  143.310   586654
    2021-07-20 09:33:00-04:00  17085 2021-07-20 09:32:00-04:00  143.535   667639
    ```
    """

    def __init__(
        self,
        asset_id_col: str,
        # TODO(gp): This should be first and also potentially be None.
        asset_ids: List[int],
        # TODO(gp): -> start_timestamp_col
        start_time_col_name: str,
        end_time_col_name: str,
        columns: Optional[List[str]],
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        # TODO(Dan): Converge on timezone `America/New_York` vs `US/Eastern` (see
        #  CMTask217).
        timezone: str = "America/New_York",
        sleep_in_secs: float = 1.0,
        time_out_in_secs: int = 60 * 2,
        column_remap: Optional[Dict[str, str]] = None,
    ):
        """
        Constructor.

        The (input) name of the columns delivered by the derived classes is set
        through `asset_id_col`, `start_time_col_name`, `end_time_col_name`.
        The (output) name of the columns can be changed through `column_remap`.

        All the column names in the interface (e.g., `start_time_col_name`) are
        before the remapping.

        :param asset_id_col: the name of the column used to select the asset ids
        :param asset_ids: as described in the class docstring
        :param start_time_col_name: the name of the column storing the `start_time`
        :param end_time_col_name: the name of the column storing the `end_time`
        :param columns: columns to return. `None` means all available
        :param get_wall_clock_time: the wall clock
        :param timezone: timezone to convert normalized output timestamps to
        :param sleep_in_secs, time_out_in_secs: sample every `sleep_in_secs`
            seconds waiting up to `time_out_in_secs` seconds
        :param column_remap: dict of columns to remap the output data or `None` for
            no remapping
        """
        _LOG.debug("")
        self._asset_id_col = asset_id_col
        dassert_valid_asset_ids(asset_ids)
        self._asset_ids = asset_ids
        self._start_time_col_name = start_time_col_name
        self._end_time_col_name = end_time_col_name
        self._columns = columns
        #
        hdbg.dassert_isinstance(get_wall_clock_time, Callable)
        self._get_wall_clock_time = get_wall_clock_time
        #
        hdbg.dassert_lt(0, sleep_in_secs)
        self._sleep_in_secs = sleep_in_secs
        #
        self._timezone = timezone
        self._column_remap = column_remap
        # Compute the max number of iterations.
        hdbg.dassert_lt(0, time_out_in_secs)
        max_iterations = int(time_out_in_secs / sleep_in_secs)
        hdbg.dassert_lte(1, max_iterations)
        self._max_iterations = max_iterations

    # /////////////////////////////////////////////////////////////////////////////

    def get_data_for_last_period(
        self,
        timedelta: pd.Timedelta,
        *,
        # TODO(gp): @Grisha not sure limit is really needed. We could move it
        # to the DB implementation.
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get an amount of data `timedelta` in the past before the current
        timestamp.

        This is used during real-time execution to evaluate a model.

        Note that we use `asset_ids` from the constructor instead of passing it
        since the use case is for clients to just ask data that has been
        configured upstream when this object was built.
        """
        # Handle `timedelta`.
        _LOG.verb_debug(hprint.to_str("timedelta"))
        wall_clock_time = self.get_wall_clock_time()
        start_ts = self._process_period(timedelta, wall_clock_time)
        end_ts = None
        # By convention to get the last chunk of data we use the start_time column.
        ts_col_name = self._start_time_col_name
        asset_ids = self._asset_ids
        # Get the data.
        df = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            limit=limit,
        )
        # We don't need to remap columns since `get_data_for_interval()` has already
        # done it.
        _LOG.verb_debug("-> df=\n%s", hpandas.df_to_str(df))
        return df

    def get_data_at_timestamp(
        self,
        ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
    ) -> pd.DataFrame:
        """
        Return price data at a specific timestamp.

        :param ts_col_name: the name of the column (before the remapping) to filter
            on and use as index
        :param ts: the timestamp to filter on
        :param asset_ids: list of asset ids to filter on. `None` for all asset ids.
        """
        dassert_valid_asset_ids(asset_ids)
        start_ts = ts - pd.Timedelta("1S")
        end_ts = ts + pd.Timedelta("1S")
        df = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
        )
        # We don't need to remap columns since `get_data_for_interval()` has already
        # done it.
        _LOG.verb_debug("-> df=\n%s", hpandas.df_to_str(df))
        return df

    def get_data_for_interval(
        self,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        *,
        left_close: bool = True,
        right_close: bool = False,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Return price data for an interval with `start_ts` and `end_ts`
        boundaries.

        All the `get_data_*` functions should go through this function since
        it is in charge of converting the data to the right timezone and
        performing the column name remapping.

        :param ts_col_name: the name of the column (before the remapping) to filter
            on
        :param asset_ids: list of asset ids to filter on. `None` for all asset ids.
        :param left_close, right_close: represent the type of interval
            - E.g., [start_ts, end_ts), or (start_ts, end_ts]
        """
        _LOG.debug(
            hprint.to_str(
                "start_ts end_ts ts_col_name asset_ids left_close right_close limit"
            )
        )
        # Resolve the asset ids.
        if asset_ids is None:
            asset_ids = self._asset_ids
        dassert_valid_asset_ids(asset_ids)
        # Check the requested interval.
        hdateti.dassert_is_valid_interval(
            start_ts, end_ts, left_close, right_close
        )
        # Delegate to the derived classes to retrieve the data.
        df = self._get_data(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close,
            right_close,
            limit,
        )
        # If the assets were specified, check that the returned data doesn't contain
        # data that we didn't request.
        hdbg.dassert_is_subset(df[self._asset_id_col].unique(), asset_ids)
        # TODO(gp): If asset_ids was specified but the backend has a universe
        #  specified already, we might need to apply a filter by asset_ids.
        # TODO(gp): Check data with respect to start_ts, end_ts.
        # Normalize data.
        df = self._normalize_data(df)
        # Convert start and end timestamps to the timezone specified in the ctor.
        df = self._convert_timestamps_to_timezone(df)
        # Remap column names.
        df = self._remap_columns(df)
        _LOG.verb_debug("-> df=\n%s", hpandas.df_to_str(df))
        hdbg.dassert_isinstance(df, pd.DataFrame)
        return df

    def get_wall_clock_time(self) -> pd.Timestamp:
        """
        Return wall clock time in the timezone specified in the ctor.

        Initially wall clock time can be in any timezone, but cannot be
        timezone-naive.
        """
        wall_clock_time = self._get_wall_clock_time()
        hdateti.dassert_has_tz(wall_clock_time)
        wall_clock_time_correct_timezone = wall_clock_time.tz_convert(
            self._timezone
        )
        return wall_clock_time_correct_timezone

    # /////////////////////////////////////////////////////////////////////////////

    def get_twap_price(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: List[int],
        column: str,
    ) -> pd.Series:
        """
        Compute TWAP of the column `column` in (ts_start, ts_end].

        E.g., TWAP for (9:30, 9:35] means avg(p(9:31), ..., p(9:35)).

        This function should be called `get_twa_price()` or `get_twap()`, but alas
        TWAP is often used as an adjective for price.
        """
        dassert_valid_asset_ids(asset_ids)
        # Get the slice (start_ts, end_ts] of prices.
        left_close = False
        right_close = True
        hdateti.dassert_is_valid_interval(
            start_ts, end_ts, left_close, right_close
        )
        prices = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close=left_close,
            right_close=right_close,
            limit=None,
        )
        # We don't need to remap columns since `get_data_for_interval()` has already
        # done it.
        hdbg.dassert_in(column, prices.columns)
        # Compute the mean value.
        _LOG.verb_debug("prices=\n%s", prices)
        twap = prices.groupby(self._asset_id_col)[column].mean()
        hpandas.dassert_series_type_in(twap, [np.float64, np.int64])
        return twap

    def get_last_twap_price(
        self,
        bar_duration: str,
        ts_col_name: str,
        asset_ids: List[int],
        column: str,
    ) -> pd.Series:
        """
        Compute TWAP of the column `column` over last `bar_duration`.

        E.g., if the last end time is 9:35 and `bar_duration=5T`, then
        we compute TWAP for (9:30, 9:35].
        """
        dassert_valid_asset_ids(asset_ids)
        last_end_time = self.get_last_end_time()
        _LOG.info("last_end_time=%s", last_end_time)
        offset = pd.Timedelta(bar_duration)
        first_end_time = last_end_time - offset
        # We rely on the assumption that we are reading 1-minute bars.
        start_time = first_end_time - pd.Timedelta(minutes=1)
        twap = self.get_twap_price(
            start_time,
            last_end_time,
            ts_col_name,
            asset_ids,
            column,
        )
        return twap

    # /////////////////////////////////////////////////////////////////////////////
    # Methods for handling real-time behaviors.
    # /////////////////////////////////////////////////////////////////////////////

    def get_last_end_time(self) -> Optional[pd.Timestamp]:
        """
        Return the last `end_time` present in the RT DB.

        In the actual RT DB there is always some data, so we return a
        timestamp. We return `None` only for replayed time when there is
        no time (e.g., before the market opens).
        """
        ret = self._get_last_end_time()
        if ret is not None:
            # Convert to ET.
            # TODO(Dan): Pass timezone from ctor in CmTask1000.
            ret = ret.tz_convert("America/New_York")
        _LOG.verb_debug("-> ret=%s", ret)
        return ret

    def get_last_price(
        self,
        col_name: str,
        asset_ids: List[int],
    ) -> pd.Series:
        """
        Get last price for `asset_ids` using column `col_name` (e.g., "close")
        """
        dassert_valid_asset_ids(asset_ids)
        # TODO(Paul): Use a to-be-written `get_last_start_time()` instead.
        last_end_time = self.get_last_end_time()
        _LOG.info("last_end_time=%s", last_end_time)
        # Get the data.
        # TODO(Paul): Remove the hard-coded 1-minute.
        start_time = last_end_time - pd.Timedelta("1T")
        df = self.get_data_at_timestamp(
            start_time,
            self._start_time_col_name,
            asset_ids,
        )
        # Convert the df of data into a series.
        hdbg.dassert_in(col_name, df.columns)
        last_price = df[[col_name, self._asset_id_col]]
        last_price.set_index(self._asset_id_col, inplace=True)
        last_price_srs = hpandas.to_series(last_price)
        hdbg.dassert_isinstance(last_price_srs, pd.Series)
        last_price_srs.index.name = self._asset_id_col
        last_price_srs.name = col_name
        hpandas.dassert_series_type_in(last_price_srs, [np.float64, np.int64])
        # TODO(gp): Print if there are nans.
        return last_price_srs

    @abc.abstractmethod
    def should_be_online(self, wall_clock_time: pd.Timestamp) -> bool:
        """
        Return whether the interface should be available at the given time.
        """
        ...

    def is_online(self) -> bool:
        """
        Return whether the DB is on-line at the current time.

        This is useful to avoid to wait on a DB that is off-line. We
        check this by checking if there was data in the last minute.
        """
        # Check if the data in the last minute is empty.
        _LOG.verb_debug("")
        # The DB is online if there was data within the last minute.
        last_db_end_time = self.get_last_end_time()
        if last_db_end_time is None:
            ret = False
        else:
            _LOG.verb_debug(
                "last_db_end_time=%s -> %s",
                last_db_end_time,
                last_db_end_time.floor("Min"),
            )
            wall_clock_time = self.get_wall_clock_time()
            _LOG.verb_debug(
                "wall_clock_time=%s -> %s",
                wall_clock_time,
                wall_clock_time.floor("Min"),
            )
            ret = last_db_end_time.floor("Min") >= (
                wall_clock_time.floor("Min") - pd.Timedelta("1T")
            )
        _LOG.verb_debug("-> ret=%s", ret)
        return ret

    async def wait_for_latest_data(
        self,
    ) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        """
        Wait until the bar with `end_time` == `wall_clock_time` is present in
        the RT DB.

        :return:
            - start_sampling_time: timestamp when the sampling started
            - end_sampling_time: timestamp when the sampling ended, since the bar
              was ready
            - num_iter: number of iterations before the last bar was ready
        """
        start_sampling_time = self.get_wall_clock_time()
        _LOG.verb_debug("DB on-line: %s", self.is_online())
        #
        hprint.log_frame(_LOG, "Waiting on last bar ...")
        num_iter = 0
        while True:
            wall_clock_time = self.get_wall_clock_time()
            last_db_end_time = self.get_last_end_time()
            # TODO(gp): We should use the new hasynci.poll().
            _LOG.debug(
                "\n### waiting on last bar: "
                "num_iter=%s/%s: wall_clock_time=%s last_db_end_time=%s",
                num_iter,
                self._max_iterations,
                wall_clock_time,
                last_db_end_time,
            )
            if last_db_end_time and (
                last_db_end_time.floor("Min") >= wall_clock_time.floor("Min")
            ):
                # Get the current timestamp when the call was finally executed.
                hprint.log_frame(_LOG, "Waiting on last bar: done")
                end_sampling_time = wall_clock_time
                break
            if num_iter >= self._max_iterations:
                raise TimeoutError
            num_iter += 1
            _LOG.verb_debug("Sleep for %s secs", self._sleep_in_secs)
            await asyncio.sleep(self._sleep_in_secs)
        _LOG.verb_debug(
            "-> %s",
            hprint.to_str("start_sampling_time end_sampling_time num_iter"),
        )
        return start_sampling_time, end_sampling_time, num_iter

    # /////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _process_period(
        timedelta: pd.Timedelta, wall_clock_time: pd.Timestamp
    ) -> Optional[pd.Timestamp]:
        """
        Return the start time corresponding to returning the desired
        `timedelta` of time before the current wall clock time.

        E.g., if the df looks like:
        ```
           start_datetime           last_price    id
                     end_datetime
                              timestamp_db
        0  09:30     09:31    09:31  -0.125460  1000
        1  09:31     09:32    09:32   0.325254  1000
        2  09:32     09:33    09:33   0.557248  1000
        3  09:33     09:34    09:34   0.655907  1000
        4  09:34     09:35    09:35   0.311925  1000
        ```
        and `wall_clock_time=09:34` the last minute `1T` should be:
        ```
           start_datetime           last_price    id
                     end_datetime
                              timestamp_db
        4  09:34     09:35    09:35   0.311925  1000
        ```

        :param timedelta: a `pd.Timedelta` like `1D`, `5T`
        """
        _LOG.verb_debug(hprint.to_str("timedelta wall_clock_time"))
        hdbg.dassert_isinstance(timedelta, pd.Timedelta)
        hdbg.dassert_lt(pd.Timedelta("0S"), timedelta)
        last_start_time = wall_clock_time - timedelta
        _LOG.verb_debug("last_start_time=%s", last_start_time)
        return last_start_time

    # /////////////////////////////////////////////////////////////////////////////
    # Derived class interface.
    # /////////////////////////////////////////////////////////////////////////////

    @abc.abstractmethod
    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        ...

    @abc.abstractmethod
    def _get_data(
        self,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        """
        Return data in the interval start_ts, end_ts for certain assets.

        This should be the only entrypoint to get data from the derived
        classes.

        :param start_ts: beginning of the time interval to select data for
        :param end_ts: end of the time interval to select data for
        :param ts_col_name: the name of the column (before the remapping) to filter
            on
        :param asset_ids: list of asset ids to filter on. `None` for all asset ids.
        :param left_close, right_close: represent the type of interval
            - E.g., [start_ts, end_ts), or (start_ts, end_ts]
        :param limit: keep only top N records
        """
        ...

    # /////////////////////////////////////////////////////////////////////////////
    # Data normalization.
    # /////////////////////////////////////////////////////////////////////////////

    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform df from real-time DB into data similar to the historical TAQ
        bars.

        The input df looks like:
        ```
          asset_id           start_time             end_time     close   volume

        idx
          0  17085  2021-07-26 13:40:00  2021-07-26 13:41:00  149.0250   575024
          1  17085  2021-07-26 13:41:00  2021-07-26 13:42:00  148.8600   400176
          2  17085  2021-07-26 13:30:00  2021-07-26 13:31:00  148.5300  1407725
        ```

        The output df looks like:
        ```
                                asset_id                start_time    close   volume
        end_time
        2021-07-20 09:31:00-04:00  17085 2021-07-20 09:30:00-04:00  143.990  1524506
        2021-07-20 09:32:00-04:00  17085 2021-07-20 09:31:00-04:00  143.310   586654
        2021-07-20 09:33:00-04:00  17085 2021-07-20 09:32:00-04:00  143.535   667639
        ```
        """
        # Sort in increasing time order and reindex.
        df.sort_values(
            [self._end_time_col_name, self._asset_id_col], inplace=True
        )
        df.set_index(self._end_time_col_name, drop=True, inplace=True)
        # TODO(gp): Add a check to make sure we are not getting data after the
        #  current time.
        _LOG.verb_debug("df.empty=%s, df.shape=%s", df.empty, str(df.shape))
        # # The data source should not return data after the current time.
        # if not df.empty:
        #     wall_clock_time = self.get_wall_clock_time()
        #     _LOG.debug(hprint.to_str("wall_clock_time df.index.max()"))
        #     hdbg.dassert_lte(df.index.max(), wall_clock_time)
        # _LOG.debug(hpandas.df_to_str(df, print_shape_info=True, tag="after process_data"))
        return df

    def _remap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remap column names with provided mapping.

        :param df: input dataframe
        :return: dataframe with remapped column names
        """
        if self._column_remap:
            hpandas.dassert_valid_remap(df.columns.tolist(), self._column_remap)
            df = df.rename(columns=self._column_remap)
        return df

    def _convert_timestamps_to_timezone(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert start and end timestamps to the specified timezone.

        :param df: normalized data
        :return: data with start and end dates in specified timezone
        """
        if df.empty:
            return df
        # Convert end timestamp values that are used as dataframe index.
        hpandas.dassert_index_is_datetime(df)
        df.index = df.index.tz_convert(self._timezone)
        # Convert start timestamp column values.
        hdbg.dassert_in(self._start_time_col_name, df.columns)
        df[self._start_time_col_name] = df[
            self._start_time_col_name
        ].dt.tz_convert(self._timezone)
        return df
