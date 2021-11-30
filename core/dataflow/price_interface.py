"""
Import as:

import core.dataflow.price_interface as cdtfprint
"""

import abc
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from IPython.display import display

import core.dataflow.real_time as cdtfretim
import core.pandas_helpers as cpanh
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


# #############################################################################
# AbstractPriceInterface
# #############################################################################


class AbstractPriceInterface(abc.ABC):
    """
    Implement an interface to an historical / real-time source of price data.

    Responsibilities:
    - Delegates to a data backend in Instrument Master (IM) to retrieve
      historical and real-time data
    - Implement RT behaviors (e.g, `is_last_bar_available`, wall_clock, ...)
        - TODO(gp): Maybe move them in IM too?
    - Stitch together different data representations (e.g., historical / RT)
      using multiple IM backends
    - Remap columns to connect data backends to consumers
    - Implement some market related transformations (e.g., TWAP)

    Non-responsibilities:
    - In general it doesn't access the data directly but relies on an Instrument
      Master object to retrieve the data from different backends

    All the timestamps in the interface are in ET timezone.
    - TODO(gp): Maybe UTC with the possibility of a switch to enforce certain
      tz?
    """
    def __init__(
        self,
        id_col_name: str,
        ids: List[Any],
        # TODO(gp): These are before the remapping.
        start_time_col_name: str,
        end_time_col_name: str,
        columns: Optional[List[str]],
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        sleep_in_secs: float = 1.0,
        time_out_in_secs: int = 60 * 2,
        column_remap: Optional[Dict[str, str]] = None,
    ):
        """
        Constructor.

        :param id_col_name: the name of the column used to select the ids
        :param ids: ids to keep
        :param start_time_col_name: the column with the start_time
        :param end_time_col_name: the column with the end_time
        :param columns: columns to return
        :param get_wall_clock_time, speed_up_factor: like in `ReplayedTime`
        :param sleep_in_secs, time_out_in_secs: sample every `sleep_in_secs`
            seconds waiting up to `time_out_in_secs` seconds
        :param column_remap: dict of columns to remap or `None`
        """
        _LOG.debug("")
        self._id_col_name = id_col_name
        self._ids = ids
        self._start_time_col_name = start_time_col_name
        self._end_time_col_name = end_time_col_name
        self._columns = columns
        hdbg.dassert_isinstance(get_wall_clock_time, Callable)
        self._get_wall_clock_time = get_wall_clock_time
        #
        hdbg.dassert_lt(0, sleep_in_secs)
        self._sleep_in_secs = sleep_in_secs
        #
        self._column_remap = column_remap
        # Compute the max number of iterations.
        max_iters = int(time_out_in_secs / sleep_in_secs)
        hdbg.dassert_lte(1, max_iters)
        self._max_iters = max_iters

    @property
    def get_wall_clock_time(self) -> hdateti.GetWallClockTime:
        return self._get_wall_clock_time

    # TODO(gp): If the DB supports asyncio this should become async.
    # TODO(gp): -> get_data_for_last_period
    def get_data(
        self,
        period: str,
        *,
        normalize_data: bool = True,
        # TODO(gp): Not sure limit is really needed. We could move it to the DB
        #  implementation.
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get data for the real-time execution where we need a certain amount of
        data since the current timestamp.

        Without normalization, the returned df is the raw data from the DB and
        could look like:
        ```
               id           start_time             end_time     close   volume
        idx  7085  2021-07-26 13:40:00  2021-07-26 13:41:00  149.0250   575024
          0  7085  2021-07-26 13:41:00  2021-07-26 13:42:00  148.8600   400176
          1  7085  2021-07-26 13:30:00  2021-07-26 13:31:00  148.5300  1407725
          2  7085  2021-07-26 13:31:00  2021-07-26 13:32:00  148.0999   473869
        ```

        After normalization, the returned df looks like:
        ```
                                     id          start_time    close   volume
        end_time
        2021-07-20 09:31:00-04:00  7085 2021-07-20 09:30:00  143.990  1524506
        2021-07-20 09:32:00-04:00  7085 2021-07-20 09:31:00  143.310   586654
        2021-07-20 09:33:00-04:00  7085 2021-07-20 09:32:00  143.535   667639
        ```
        """
        # Handle `period`.
        _LOG.debug(hprint.to_str("period"))
        current_time = self._get_wall_clock_time()
        start_ts = _process_period(period, current_time)
        end_ts = None
        # By convention to get the last chunk of data we use the start_time column.
        ts_col_name = self._start_time_col_name
        asset_ids = self._ids
        # Get the data.
        df = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            normalize_data=normalize_data,
            limit=limit,
        )
        _LOG.debug("-> df=\n%s", hprint.dataframe_to_str(df))
        return df

    def get_data_at_timestamp(
        self,
        ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        *,
        normalize_data: bool = True,
    ) -> pd.DataFrame:
        """
        Return price data at a specific timestamp.

        :param ts_col_name: the name of the column (before the remapping) to filter
            on
        :param ts: the timestamp to filter on
        :param asset_ids: list of ids to filter on. `None` for all ids.
        """
        start_ts = ts - pd.Timedelta(1, unit="ns")
        end_ts = ts + pd.Timedelta(1, unit="ns")
        df = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            normalize_data=normalize_data,
        )
        _LOG.debug("-> df=\n%s", hprint.dataframe_to_str(df))
        return df

    def get_data_for_interval(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        *,
        left_close: bool = True,
        right_close: bool = False,
        normalize_data: bool = True,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Return price data in [start_ts, end_ts).

        :param ts_col_name: the name of the column (before the remapping) to filter
            on
        :param asset_ids: list of ids to filter on. `None` for all ids.
        :param left_close, right_close: represent the type of interval
            - E.g., [start_ts, end_ts), or (start_ts, end_ts]
        """
        if start_ts is not None and end_ts is not None:
            # TODO(gp): This should be function of right_close and left_close.
            hdbg.dassert_lt(start_ts, end_ts)
        df = self._get_data(
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close,
            right_close,
            normalize_data,
            limit,
        )
        _LOG.debug("-> df=\n%s", hprint.dataframe_to_str(df))
        return df

    def get_twap_price(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_id: int,
        column: str,
    ) -> float:
        """
        Compute TWAP of the column `column` in (ts_start, ts_end].

        E.g., TWAP for (9:30, 9:35] means avg(p(9:31), ..., p(9:35)).

        This function should be called `get_twa_price()` or `get_twap()`, but alas
        TWAP is often used as an adjective for price.
        """
        # Get the slice (start_ts, end_ts] of prices.
        left_close = False
        right_close = True
        prices = self.get_data_for_interval(
            start_ts,
            end_ts,
            ts_col_name,
            [asset_id],
            left_close=left_close,
            right_close=right_close,
            normalize_data=True,
            limit=None,
        )
        hdbg.dassert_in(column, prices.columns)
        prices = prices[column]
        # Compute the mean value.
        _LOG.debug("prices=\n%s", prices)
        price: float = prices.mean()
        hdbg.dassert(np.isfinite(price), "price=%s", price)
        return price

    # TODO(gp): -> _normalize_bar_data?
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform df from real-time DB into data similar to the historical TAQ
        bars.

        The input df looks like:
        ```
          asset_id           start_time             end_time     close   volume

        idx  17085  2021-07-26 13:40:00  2021-07-26 13:41:00  149.0250   575024
          0  17085  2021-07-26 13:41:00  2021-07-26 13:42:00  148.8600   400176
          1  17085  2021-07-26 13:30:00  2021-07-26 13:31:00  148.5300  1407725
          2  17085  2021-07-26 13:31:00  2021-07-26 13:32:00  148.0999   473869
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
        df.sort_values([self._end_time_col_name, self._id_col_name], inplace=True)
        df.set_index(self._end_time_col_name, drop=True, inplace=True)
        # TODO(gp): Add a check to make sure we are not getting data after the
        #  current time.
        # _LOG.debug("df.empty=%s, df.shape=%s", df.empty, str(df.shape))
        # # The data source should not return data after the current time.
        # if not df.empty:
        #     current_time = self._get_current_time()
        #     _LOG.debug(hprint.to_str("current_time df.index.max()"))
        #     hdbg.dassert_lte(df.index.max(), current_time)
        # _LOG.debug(hprint.df_to_short_str("after process_data", df))
        return df

    # Methods for handling real-time behaviors.

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
            ret = ret.tz_convert("America/New_York")
        _LOG.debug("-> ret=%s", ret)
        return ret

    @abc.abstractmethod
    def should_be_online(self, current_time: pd.Timestamp) -> bool:
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
        _LOG.debug("")
        # The DB is online if there was data within the last minute.
        last_db_end_time = self.get_last_end_time()
        if last_db_end_time is None:
            ret = False
        else:
            _LOG.debug(
                "last_db_end_time=%s -> %s",
                last_db_end_time,
                last_db_end_time.floor("Min"),
            )
            current_time = self._get_wall_clock_time()
            _LOG.debug(
                "current_time=%s -> %s", current_time, current_time.floor("Min")
            )
            ret = last_db_end_time.floor("Min") >= (
                current_time.floor("Min") - pd.Timedelta(minutes=1)
            )
        _LOG.debug("-> ret=%s", ret)
        return ret

    # TODO(gp): -> wait_for_latest_data
    async def is_last_bar_available(
        self,
    ) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        """
        Wait until the bar with `end_time` == `current_time` is present in the
        RT DB.

        :return:
            - start_sampling_time: timestamp when the sampling started
            - end_sampling_time: timestamp when the sampling ended, since the bar
              was ready
            - num_iters: number of iterations
        """
        start_sampling_time = self._get_wall_clock_time()
        _LOG.debug("DB on-line: %s", self.is_online())
        #
        _LOG.debug("Waiting on last bar ...")
        num_iter = 0
        while True:
            current_time = self._get_wall_clock_time()
            last_db_end_time = self.get_last_end_time()
            _LOG.debug(
                "\n%s",
                hprint.frame(
                    "Waiting on last bar: "
                    "num_iter=%s/%s: current_time=%s last_db_end_time=%s"
                    % (num_iter, self._max_iters, current_time, last_db_end_time),
                    char1="-",
                ),
            )
            if last_db_end_time and (
                last_db_end_time.floor("Min") >= current_time.floor("Min")
            ):
                # Get the current timestamp when the call was finally executed.
                _LOG.debug("Waiting on last bar: done")
                end_sampling_time = current_time
                break
            if num_iter >= self._max_iters:
                raise TimeoutError
            num_iter += 1
            _LOG.debug("Sleep for %s secs", self._sleep_in_secs)
            await asyncio.sleep(self._sleep_in_secs)
        _LOG.debug(
            "-> %s",
            hprint.to_str("start_sampling_time end_sampling_time num_iter"),
        )
        return start_sampling_time, end_sampling_time, num_iter

    @abc.abstractmethod
    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        ...

    @abc.abstractmethod
    def _get_data(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        normalize_data: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        """
        Return data in [start_ts, end_ts) for certain assets.

        This is the only entrypoint to get data from the derived classes.
        """
        ...

    def _remap_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._column_remap:
            hpandas.dassert_valid_remap(df.columns.tolist(), self._column_remap)
            df.rename(columns=self._column_remap, inplace=True)
        return df


# #############################################################################
# SqlPriceInterface
# #############################################################################


# TODO(gp): This should be pushed to the IM
class SqlPriceInterface(AbstractPriceInterface):
    """
    Implement an interface to a real-time SQL database with 1-minute bar data.
    """

    def __init__(
        self,
        dbname: str,
        host: str,
        port: int,
        user: str,
        password: str,
        table_name: str,
        where_clause: Optional[str],
        valid_id: Any,
        # Params from `AbstractPriceInterface`.
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Constructor.

        :param table_name: the table to use to get the data
        :param where_clause: an SQL where clause
            - E.g., `WHERE ...=... AND ...=...`
        """
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.connection = hsql.get_connection(
            host=host,
            dbname=dbname,
            port=port,
            user=user,
            password=password,
        )
        self.cursor = self.connection.cursor()
        self._table_name = table_name
        self._where_clause = where_clause
        self._valid_id = valid_id

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Add new TZ-localized datetime columns for research and readability.
        for col_name in [self._start_time_col_name, self._end_time_col_name]:
            if col_name in df.columns:
                srs = df[col_name]
                # _LOG.debug("srs=\n%s", str(srs.head(3)))
                if not srs.empty:
                    srs = srs.apply(pd.to_datetime)
                    srs = srs.dt.tz_localize("UTC")
                    srs = srs.dt.tz_convert("America/New_York")
                    df[col_name] = srs
        # Sort in increasing time order and reindex.
        df = super().process_data(df)
        return df

    def should_be_online(self, current_time: pd.Timestamp) -> bool:
        return True

    def _get_data(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        normalize_data: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        sort_time = True
        query = self._get_sql_query(
            self._columns,
            start_ts,
            end_ts,
            ts_col_name,
            asset_ids,
            left_close,
            right_close,
            sort_time,
            limit,
        )
        _LOG.info("query=%s", query)
        df = hsql.execute_query_to_df(self.connection, query)
        if normalize_data:
            df = self.process_data(df)
        return df

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        """
        Return the last `end_time` available in the DB.
        """
        # We assume that all the bars are inserted together in a single
        # transaction, so we can check for the max timestamp.
        # Get the latest `start_time` (which is an index) with a query like:
        #   ```
        #   SELECT MAX(start_time)
        #     FROM bars_qa
        #     WHERE interval=60 AND region='AM' AND asset_id = '17085'
        #   ```
        query = []
        query.append(f"SELECT MAX({self._start_time_col_name})")
        query.append(f"FROM {self._table_name}")
        query.append("WHERE")
        if self._where_clause:
            query.append(f"{self._where_clause} AND")
        query.append(f"{self._id_col_name} = '{self._valid_id}'")
        query = " ".join(query)
        # _LOG.debug("query=%s", query)
        df = hsql.execute_query_to_df(self.connection, query)
        # Check that the `start_time` is a single value.
        hdbg.dassert_eq(df.shape, (1, 1))
        start_time = df.iloc[0, 0]
        # _LOG.debug("start_time from DB=%s", start_time)
        # Get the `end_time` that corresponds to the last `start_time` with a
        # query like:
        #   ```
        #   SELECT end_time
        #     FROM bars_qa
        #     WHERE interval=60 AND
        #         region='AM' AND
        #         start_time = '2021-10-07 15:50:00' AND
        #         asset_id = '17085'
        #   ```
        query = []
        query.append(f"SELECT {self._end_time_col_name}")
        query.append(f"FROM {self._table_name}")
        query.append("WHERE")
        if self._where_clause:
            query.append(f"{self._where_clause} AND")
        query.append(
            f"{self._start_time_col_name} = '{start_time}' AND "
            + f"{self._id_col_name} = '{self._valid_id}'"
        )
        query = " ".join(query)
        # _LOG.debug("query=%s", query)
        df = hsql.execute_query_to_df(self.connection, query)
        # Check that the `end_time` is a single value.
        hdbg.dassert_eq(df.shape, (1, 1))
        end_time = df.iloc[0, 0]
        # _LOG.debug("end_time from DB=%s", end_time)
        # We know that it should be `end_time = start_time + 1 minute`.
        start_time = pd.Timestamp(start_time, tz="UTC")
        end_time = pd.Timestamp(end_time, tz="UTC")
        hdbg.dassert_eq(end_time, start_time + pd.Timedelta(minutes=1))
        return end_time

    # TODO(gp): Rename ids -> asset_ids.
    def _get_sql_query(
        self,
        columns: Optional[List[str]],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: List[Any],
        # TODO(gp): Move these close to start_ts.
        left_close: bool,
        right_close: bool,
        sort_time: bool,
        limit: Optional[int],
    ) -> str:
        """
        Build a query for the RT DB.

        SELECT * \
            FROM bars \
            WHERE ... AND id in (...) \
            ORDER BY end_time DESC \
            LIMIT ...

        :param columns: columns to select from `table_name`
            - `None` means all columns.
        :param asset_ids: ids to select
        :param period: what period to retrieve
            - E.g., `all`, `last_day`, `last_5mins`, `last_1min`
        :param sort_time: whether to sort by end_time
        :param limit: how many rows to return
        """
        query = []
        # Handle `columns`.
        if columns is None:
            columns_as_str = "*"
        else:
            columns_as_str = ",".join(columns)
        query.append(f"SELECT {columns_as_str} FROM {self._table_name}")
        # Handle `where` clause.
        if self._where_clause is not None:
            # E.g., "WHERE interval=60 AND region='AM'")
            query.append(f"WHERE {self._where_clause}")
        # Handle `ids`.
        hdbg.dassert_isinstance(asset_ids, list)
        if len(asset_ids) == 1:
            ids_as_str = f"{self._id_col_name}={asset_ids[0]}"
        else:
            ids_as_str = ",".join(map(str, asset_ids))
            ids_as_str = f"{self._id_col_name} in ({ids_as_str})"
        query.append("AND " + ids_as_str)
        # Handle `period`.
        if start_ts is not None:
            if left_close:
                operator = ">="
            else:
                operator = ">"
            query.append(
                f"AND {ts_col_name} {operator} "
                + "'%s'" % _to_sql_datetime_string(start_ts)
            )
        if end_ts is not None:
            if right_close:
                operator = "<="
            else:
                operator = "<"
            query.append(
                f"AND {ts_col_name} {operator} "
                + "'%s'" % _to_sql_datetime_string(end_ts)
            )
        # Handle `sort_time`.
        if sort_time:
            query.append("ORDER BY end_time DESC")
        # Handle `limit`.
        if limit is not None:
            query.append(f"LIMIT {limit}")
        query = " ".join(query)
        return query


# #############################################################################
# ReplayedTimePriceInterface
# #############################################################################


# TODO(gp): This should have a delay and / or we should use timestamp_db.
class ReplayedTimePriceInterface(AbstractPriceInterface):
    """
    Implement an interface to a replayed time historical / RT database.

    Another approach to achieve the same goal is to mock the IM directly instead
    of this class.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        knowledge_datetime_col_name: str,
        delay_in_secs: int,
        # Params from `AbstractPriceInterface`.
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        """
        Constructor.

        :param df: dataframe in the same format of an SQL based data (i.e., not in
            dataflow format, e.g., indexed by `end_time`)
        :param knowledge_datetime_col_name: column with the knowledge time for the
            corresponding data
        :param delay_in_secs: how many seconds to wait beyond the timestamp in
            `knowledge_datetime_col_name`
        """
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._df = df
        self._knowledge_datetime_col_name = knowledge_datetime_col_name
        hdbg.dassert_lte(0, delay_in_secs)
        self._delay_in_secs = delay_in_secs
        self._allow_future_peeking = False
        #
        hdbg.dassert_is_subset(
            [
                self._id_col_name,
                self._start_time_col_name,
                self._end_time_col_name,
                self._knowledge_datetime_col_name,
            ],
            df.columns,
        )
        self._df.sort_values(
            [self._end_time_col_name, self._id_col_name], inplace=True
        )

    def set_allow_future_peeking(self, val: bool) -> bool:
        old_value = val
        self._allow_future_peeking = val
        return old_value

    def should_be_online(self, current_time: pd.Timestamp) -> bool:
        return True

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        _LOG.debug("")
        # Sort in increasing time order and reindex.
        df = super().process_data(df)
        return df

    def _get_data(
        self,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        ts_col_name: str,
        asset_ids: Optional[List[int]],
        left_close: bool,
        right_close: bool,
        normalize_data: bool,
        limit: Optional[int],
    ) -> pd.DataFrame:
        _LOG.debug(
            hprint.to_str(
                "start_ts end_ts ts_col_name asset_ids left_close right_close normalize_data limit"
            )
        )
        # Filter the data by the current time.
        current_time = self._get_wall_clock_time()
        _LOG.debug(hprint.to_str("current_time"))
        df_tmp = cdtfretim.get_data_as_of_datetime(
            self._df,
            self._knowledge_datetime_col_name,
            current_time,
            delay_in_secs=self._delay_in_secs,
            allow_future_peeking=self._allow_future_peeking,
        )
        # Handle `columns`.
        if self._columns is not None:
            hdbg.dassert_is_subset(self._columns, df_tmp.columns)
            df_tmp = df_tmp[self._columns]
        # # Handle `period`.
        # TODO(gp): This is inefficient. Make it faster by binary search.
        if start_ts is not None:
            # _LOG.debug("start_ts=%s", start_ts)
            hdbg.dassert_in(ts_col_name, df_tmp)
            tss = df_tmp[ts_col_name]
            # _LOG.debug("tss=\n%s", hprint.dataframe_to_str(tss))
            if left_close:
                mask = tss >= start_ts
            else:
                mask = tss > start_ts
            # _LOG.debug("mask=\n%s", hprint.dataframe_to_str(mask))
            df_tmp = df_tmp[mask]
        if end_ts is not None:
            # _LOG.debug("end_ts=%s", end_ts)
            hdbg.dassert_in(ts_col_name, df_tmp)
            tss = df_tmp[ts_col_name]
            # _LOG.debug("tss=\n%s", hprint.dataframe_to_str(tss))
            if right_close:
                mask = tss <= end_ts
            else:
                mask = tss < end_ts
            # _LOG.debug("mask=\n%s", hprint.dataframe_to_str(mask))
            df_tmp = df_tmp[mask]
        # Handle `ids`
        # _LOG.debug("before df_tmp=\n%s", hprint.dataframe_to_str(df_tmp))
        if asset_ids is not None:
            hdbg.dassert_in(self._id_col_name, df_tmp)
            mask = df_tmp[self._id_col_name].isin(set(asset_ids))
            df_tmp = df_tmp[mask]
        # _LOG.debug("after df_tmp=\n%s", hprint.dataframe_to_str(df_tmp))
        # Handle `limit`.
        if limit:
            hdbg.dassert_lte(1, limit)
            df_tmp = df_tmp.head(limit)
        # Normalize data.
        if normalize_data:
            df_tmp = self.process_data(df_tmp)
        _LOG.debug("-> df_tmp=\n%s", hprint.dataframe_to_str(df_tmp))
        return df_tmp

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        # We need to find the last timestamp before the current time. We use
        # `last_week` but could also use all the data since we don't call the
        # DB.
        period = "last_week"
        df = self.get_data(period)
        _LOG.debug(hprint.df_to_short_str("after get_data", df))
        if df.empty:
            ret = None
        else:
            ret = df.index.max()
        return ret


# #############################################################################
# Utils.
# #############################################################################


# TODO(gp): These should be methods of AbstractPriceInterface.
def _to_sql_datetime_string(dt: pd.Timestamp) -> str:
    """
    Convert a timestamp into an SQL string to query the DB.
    """
    hdateti.dassert_has_tz(dt)
    # Convert to UTC, if needed.
    if dt.tzinfo != hdateti.get_UTC_tz().zone:
        dt = dt.tz_convert(hdateti.get_UTC_tz())
    ret: str = dt.strftime("%Y-%m-%d %H:%M:%S")
    return ret


def _process_period(
    period: str, current_time: pd.Timestamp
) -> Optional[pd.Timestamp]:
    """
    Return the start time corresponding to getting the desired `period` of
    time.

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
    and `current_time=09:34` the last minute should be
    ```
       start_datetime           last_price    id
                 end_datetime
                          timestamp_db
    4  09:34     09:35    09:35   0.311925  1000
    ```

    :param period: what period the df to extract (e.g., `last_1mins`, ...,
        `last_10mins`)
    :return:
    """
    _LOG.debug(hprint.to_str("period current_time"))
    # Period of time.
    if period == "last_day":
        # Get the data for the last day.
        last_start_time = current_time.replace(hour=0, minute=0, second=0)
    elif period == "last_week":
        # Get the data for the last day.
        last_start_time = current_time.replace(
            hour=0, minute=0, second=0
        ) - pd.Timedelta(days=16)
    elif period in ("last_10mins", "last_5mins", "last_1min"):
        # Get the data for the last N minutes.
        if period == "last_10mins":
            mins = 10
        elif period == "last_5mins":
            mins = 5
        elif period == "last_1min":
            mins = 1
        else:
            raise ValueError("Invalid period='%s'" % period)
        # We condition on `start_time` since it's an index.
        last_start_time = current_time - pd.Timedelta(minutes=mins)
    elif period == "all":
        last_start_time = None
    else:
        raise ValueError("Invalid period='%s'" % period)
    _LOG.debug("last_start_time=%s", last_start_time)
    return last_start_time


# #############################################################################
# Serialize / deserialize example of DB.
# #############################################################################


def save_raw_data(
    rtdbi: AbstractPriceInterface,
    file_name: str,
    period: str,
    limit: int,
) -> None:
    """
    Save a few days worth of bar data from the RT DB to a file.
    """
    normalize_data = False
    # Around 3 days.
    _LOG.info("Querying DB ...")
    rt_df = rtdbi.get_data(period, normalize_data=normalize_data, limit=limit)
    _LOG.info("Querying DB done")
    print("rt_df.shape=%s" % str(rt_df.shape))
    print("# head")
    display(rt_df.head(2))
    print("# tail")
    display(rt_df.tail(2))
    #
    _LOG.info("Saving ...")
    compression = None
    if file_name.endswith(".gz"):
        compression = "gzip"
    rt_df.to_csv(file_name, compression=compression, index=True)
    _LOG.info("Saving done")


def read_data_from_file(
    file_name: str,
    aws_profile: Optional[str] = None,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Load some example data from the RT DB.

    Same interface as `get_real_time_bar_data()`.

    ```
          start_time          end_time asset_id   close    volume         timestamp_db
    2021-10-05 20:00  2021-10-05 20:01    17085  141.11   5792204  2021-10-05 20:01:03
    2021-10-05 19:59  2021-10-05 20:00    17085  141.09   1354151  2021-10-05 20:00:05
    2021-10-05 19:58  2021-10-05 19:59    17085  141.12    620395  2021-10-05 19:59:04
    2021-10-05 19:57  2021-10-05 19:58    17085  141.2644  341584  2021-10-05 19:58:03
    2021-10-05 19:56  2021-10-05 19:57    17085  141.185   300822  2021-10-05 19:57:04
    2021-10-05 19:55  2021-10-05 19:56    17085  141.1551  351527  2021-10-05 19:56:04
    ```
    """
    kwargs_tmp = {
        "index_col": 0,
        "parse_dates": ["start_time", "end_time", "timestamp_db"],
    }
    if aws_profile:
        s3fs_ = hs3.get_s3fs(aws_profile)
        kwargs_tmp["s3fs"] = s3fs_
    kwargs.update(kwargs_tmp)  # type: ignore[arg-type]
    df = cpanh.read_csv(file_name, **kwargs)
    return df


# #############################################################################
# Stats about DB delay
# #############################################################################


def compute_rt_delay(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a "delay" column with the difference between `timestamp_db` and
    `end_time`.
    """

    def _to_et(srs: pd.Series) -> pd.Series:
        srs = srs.dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        return srs

    timestamp_db = _to_et(df["timestamp_db"])
    end_time = _to_et(df["end_time"])
    # Compute delay.
    delay = (timestamp_db - end_time).dt.total_seconds()
    delay = round(delay, 2)
    df["delay_in_secs"] = delay
    return df


def describe_rt_delay(df: pd.DataFrame) -> None:
    """
    Compute some statistics for the DB delay.
    """
    delay = df["delay_in_secs"]
    print("delays=%s" % hprint.format_list(delay, max_n=5))
    if False:
        print(
            "delays.value_counts=\n%s" % hprint.indent(str(delay.value_counts()))
        )
    delay.plot.hist()


def describe_rt_df(df: pd.DataFrame, *, include_delay_stats: bool) -> None:
    """
    Print some statistics for a df from the RT DB.
    """
    print("shape=%s" % str(df.shape))
    # Is it sorted?
    end_times = df["end_time"]
    is_sorted = all(sorted(end_times, reverse=True) == end_times)
    print("is_sorted=", is_sorted)
    # Stats about `end_time`.
    min_end_time = df["end_time"].min()
    max_end_time = df["end_time"].max()
    num_mins = df["end_time"].nunique()
    print(
        "end_time: num_mins=%s [%s, %s]" % (num_mins, min_end_time, max_end_time)
    )
    # Stats about `asset_ids`.
    # TODO(gp): Pass the name of the column through the interface.
    print("asset_ids=%s" % hprint.format_list(df["egid"].unique()))
    # Stats about delay.
    if include_delay_stats:
        df = compute_rt_delay(df)
        describe_rt_delay(df)
    #
    display(df.head(3))
    display(df.tail(3))
