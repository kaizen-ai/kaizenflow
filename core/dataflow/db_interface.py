"""
Import as:

import core.dataflow.db_interface as cdtfdbint
"""

import abc
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from IPython.display import display

import core.dataflow.real_time as cdtfretim
import core.pandas_helpers as cpah
import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.printing as hprintin
import helpers.s3 as hs3
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


# #############################################################################
# RealTimeDbInterface
# #############################################################################


# TODO(gp): -> Abstract...?
class RealTimeDbInterface(abc.ABC):
    """
    Implement an interface to a real-time database with 1-minute bar data.

    All the timestamps in the interface are in ET timezone.
    """

    def __init__(
        self,
        id_col_name: str,
        ids: List[Any],
        start_time_col_name: str,
        end_time_col_name: str,
        columns: Optional[List[str]],
        get_wall_clock_time: hdatetim.GetWallClockTime,
        *,
        sleep_in_secs: float = 1.0,
        time_out_in_secs: int = 60 * 2,
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
        # Compute the max number of iterations.
        max_iters = int(time_out_in_secs / sleep_in_secs)
        hdbg.dassert_lte(1, max_iters)
        self._max_iters = max_iters

    # TODO(gp): If the DB supports asyncio this should become async.
    @abc.abstractmethod
    def get_data(
        self,
        period: str,
        *,
        normalize_data: bool = True,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get data from the real-time DB.

        Without normalization, the returned df is the raw data from the DB and could
        look like:
        ```
                id           start_time             end_time     close   volume

        idx  7085  2021-07-26 13:40:00  2021-07-26 13:41:00  149.0250   575024
          0  7085  2021-07-26 13:41:00  2021-07-26 13:42:00  148.8600   400176
          1  7085  2021-07-26 13:30:00  2021-07-26 13:31:00  148.5300  1407725
          2  7085  2021-07-26 13:31:00  2021-07-26 13:32:00  148.0999   473869
        ```

        After normalization, the returned df looks like:
        ```
                                     id                start_time    close   volume
        end_time
        2021-07-20 09:31:00-04:00  7085 2021-07-20 09:30:00-04:00  143.990  1524506
        2021-07-20 09:32:00-04:00  7085 2021-07-20 09:31:00-04:00  143.310   586654
        2021-07-20 09:33:00-04:00  7085 2021-07-20 09:32:00-04:00  143.535   667639
        ```
        """
        ...

    # TODO(gp): -> _normalize_bar_data?
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform df from real-time DB into data similar to the historical TAQ
        bars.

        The input df looks like:
        ```
              egid           start_time             end_time     close   volume

        idx  17085  2021-07-26 13:40:00  2021-07-26 13:41:00  149.0250   575024
          0  17085  2021-07-26 13:41:00  2021-07-26 13:42:00  148.8600   400176
          1  17085  2021-07-26 13:30:00  2021-07-26 13:31:00  148.5300  1407725
          2  17085  2021-07-26 13:31:00  2021-07-26 13:32:00  148.0999   473869
        ```

        The output df looks like:
        ```
                                    egid                start_time    close   volume
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
        #     _LOG.debug(hprintin.to_str("current_time df.index.max()"))
        #     hdbg.dassert_lte(df.index.max(), current_time)
        _LOG.debug(hprintin.df_to_short_str("after process_data", df))
        return df

    @abc.abstractmethod
    def get_last_end_time(self) -> pd.Timestamp:
        """
        Return the last `end_time` in the RT DB.

        We assume that all the bars are inserted together in a single transaction,
        so we can check for a single id, e.g., AAPL.

        :return: the timestamp is in ET like everything in the RT DB
        """
        ...

    def is_online(self) -> bool:
        """
        Return whether the DB is on-line at the current time.

        This is useful to avoid to wait on a DB that is off-line.
        """
        # Check if the data in the last minute is empty.
        return self.get_last_end_time() is not None

    # TODO(gp): -> wait_for_latest_data
    async def is_last_bar_available(
        self,
    ) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        """
        Wait until the bar with `end_time` == `current_time` is present in the
        RT DB.

        :return:
            - start_time: timestamp when the sampling started
            - current_time: timestamp when the bar ended
            - num_iters: number of iterations
        """
        _LOG.debug("")
        start_time = self._get_wall_clock_time()
        _LOG.debug("DB on-line: %s", self.is_online())
        #
        num_iter = 0
        while True:
            current_time = self._get_wall_clock_time()
            last_end_time = self.get_last_end_time()
            _LOG.debug(
                "\n%s",
                hprintin.frame(
                    "current_time=%s: num_iter=%s/%s: last_end_time=%s"
                    % (current_time, num_iter, self._max_iters, last_end_time)
                ),
            )
            if last_end_time and (
                last_end_time.floor("Min") >= current_time.floor("Min")
            ):
                # Get the current timestamp when the call was finally executed.
                _LOG.debug("Done")
                break
            if num_iter >= self._max_iters:
                raise TimeoutError
            num_iter += 1
            await asyncio.sleep(self._sleep_in_secs)
        return start_time, current_time, num_iter


# #############################################################################
# RealTimeSqlDbInterface
# #############################################################################


class RealTimeSqlDbInterface(RealTimeDbInterface):
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
        #
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
        self.connection, self.cursor = hsql.get_connection(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password,
        )
        self._table_name = table_name
        self._where_clause = where_clause
        self._valid_id = valid_id

    def get_data(
        self,
        period: str,
        *,
        normalize_data: bool = True,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        sort_time = True
        query = self._get_sql_query(
            self._columns, self._ids, period, sort_time, limit
        )
        _LOG.info("query=%s", query)
        df = hsql.execute_query(self.connection, query)
        if normalize_data:
            df = self.process_data(df)
        return df

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

    def get_last_end_time(self) -> pd.Timestamp:
        # Get the latest `start_time`.
        query = []
        query.append(f"SELECT MAX({self._start_time_col_name})")
        query.append(f"FROM {self._table_name}")
        query.append("WHERE")
        if self._where_clause:
            query.append(f"{self._where_clause} AND")
        query.append(f"{self._id_col_name} = '{self._valid_id}'")
        query = " ".join(query)
        df = hsql.execute_query(self.connection, query)
        # Check that the `start_time` is a single value.
        hdbg.dassert_eq(df.shape, (1, 1))
        start_time = df.iloc[0, 0]
        _LOG.debug("start_time=%s", start_time)
        # Get the `end_time` that corresponds to the last `start_time`.
        query = []
        query.append(f"SELECT {self._end_time_col_name}")
        query.append(f"FROM {self._table_name}")
        query.append("WHERE")
        if self._where_clause:
            query.append(f"{self._where_clause} AND")
        query.append(
            f"{self._start_time_col_name} = '{start_time}' AND "
            + f"{self._where_clause} AND "
            + f"{self._id_col_name} = '{self._valid_id}'"
        )
        query = " ".join(query)
        df = hsql.execute_query(self.connection, query)
        # Check that the `end_time` is a single value.
        hdbg.dassert_eq(df.shape, (1, 1))
        end_time = df.iloc[0, 0]
        _LOG.debug("end_time=%s", end_time)
        # We know that it should be `end_time = start_time + 1 minute`.
        start_time = pd.Timestamp(start_time, tz="UTC")
        end_time = pd.Timestamp(end_time, tz="UTC")
        hdbg.dassert_eq(end_time, start_time + pd.Timedelta(minutes=1))
        # Convert to ET.
        end_time = end_time.tz_convert("America/New_York")
        return end_time

    def _get_sql_query(
        self,
        columns: Optional[List[str]],
        ids: List[Any],
        period: str,
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
        :param ids: ids to select
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
        hdbg.dassert_isinstance(ids, list)
        if len(ids) == 1:
            ids_as_str = f"{self._id_col_name}={ids[0]}"
        else:
            ids_as_str = ",".join(map(str, ids))
            ids_as_str = f"{self._id_col_name} in ({ids_as_str})"
        query.append("AND " + ids_as_str)
        # Handle `period`.
        current_time = hdatetim.get_current_time(tz="UTC")
        last_start_time = _process_period(period, current_time)
        if last_start_time is not None:
            query.append(
                f"AND {self._start_time_col_name} >= "
                + "'%s'" % _to_sql_datetime_string(last_start_time)
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
# ReplayedTimeDbInterface
# #############################################################################


# TODO(gp): This should have a delay and / or we should use timestamp_db.
class ReplayedTimeDbInterface(RealTimeDbInterface):
    """
    Implement an interface to a replayed time database with 1-minute bar data.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        knowledge_datetime_col_name: str,
        delay_in_secs: int,
        #
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

    def get_data(
        self,
        period: str,
        *,
        normalize_data: bool = True,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        _LOG.debug(hprintin.to_str("period"))
        # Filter the data by the current time.
        current_time = self._get_wall_clock_time()
        _LOG.debug(hprintin.to_str("current_time"))
        df_tmp = cdtfretim.get_data_as_of_datetime(
            self._df,
            self._knowledge_datetime_col_name,
            current_time,
            delay_in_secs=self._delay_in_secs,
        )
        # Handle `columns`.
        if self._columns is not None:
            hdbg.dassert_is_subset(self._columns, df_tmp.columns)
            df_tmp = df_tmp[self._columns]
        # Handle `period`.
        current_time = self._get_wall_clock_time()
        last_start_time = _process_period(period, current_time)
        if last_start_time is not None:
            mask = df_tmp[self._start_time_col_name] >= last_start_time
            df_tmp = df_tmp[mask]
        # Handle `ids`
        mask = df_tmp[self._id_col_name].isin(set(self._ids))
        df_tmp = df_tmp[mask]
        # Handle `limit`.
        if limit:
            hdbg.dassert_lte(1, limit)
            df_tmp = df_tmp.head(limit)
        # Normalize data.
        if normalize_data:
            df_tmp = self.process_data(df_tmp)
        return df_tmp

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        _LOG.debug("")
        # Sort in increasing time order and reindex.
        df = super().process_data(df)
        return df

    def get_last_end_time(self) -> Optional[pd.Timestamp]:
        _LOG.debug("")
        # We need to find the last timestamp before the current time.
        period = "last_1min"
        df = self.get_data(period)
        _LOG.debug(hprintin.df_to_short_str("after get_data", df))
        if df.empty:
            ret = None
        else:
            ret = df.index.max()
        return ret


# #############################################################################
# Utils.
# #############################################################################


def _to_sql_datetime_string(dt: pd.Timestamp) -> str:
    """
    Convert a UTC timestamp into an SQL string to query the DB.
    """
    hdatetim.dassert_has_UTC_tz(dt)
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
    _LOG.debug(hprintin.to_str("period current_time"))
    # Period of time.
    if period == "last_day":
        # Get the data for the last day.
        # TODO(gp): We should use the current time that works for both real and
        # simulated mode.
        current_time = hdatetim.get_current_time(tz="UTC")
        last_start_time = current_time.floor(freq="1D")
    elif period in ("last_10mins", "last_5mins", "last_1min"):
        # Get the data for the last N minutes.
        # current_time = hdatetim.get_current_time(tz="UTC")
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
        _LOG.debug("last_start_time=%s", last_start_time)
    elif period == "all":
        last_start_time = None
    else:
        raise ValueError("Invalid period='%s'" % period)
    return last_start_time


# #############################################################################
# Serialize / deserialize example of DB.
# #############################################################################


def save_raw_data(
    rtdbi: RealTimeDbInterface,
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
    print("# head")
    display(rt_df.head(2))
    print("# tail")
    display(rt_df.tail(2))
    #
    _LOG.info("Saving ...")
    rt_df.to_csv(file_name, compression="gzip", index=False)
    _LOG.info("Saving done")


def read_data_from_file(
    file_name: str,
    aws_profile: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load some example data from the RT DB.

    Same interface as `get_real_time_bar_data()`.
    """
    if aws_profile:
        s3fs_ = hs3.get_s3fs(aws_profile)
    else:
        s3fs_ = None
    kwargs = {  # "index_col": 0,
        "parse_dates": ["start_time", "end_time", "timestamp_db"],
        "s3fs": s3fs_,
    }
    df = cpah.read_csv(file_name, **kwargs)
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
    print("delays=%s" % hprintin.format_list(delay, max_n=5))
    if False:
        print(
            "delays.value_counts=\n%s"
            % hprintin.indent(str(delay.value_counts()))
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
    # Stats about `egids`.
    print("egids=%s" % hprintin.format_list(df["egid"].unique()))
    # Stats about delay.
    if include_delay_stats:
        df = compute_rt_delay(df)
        describe_rt_delay(df)
    #
    display(df.head(3))
    display(df.tail(3))
