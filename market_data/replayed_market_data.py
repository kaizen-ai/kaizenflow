"""
Import as:

import market_data.replayed_market_data as mdremada
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import core.pandas_helpers as cpanh
import core.real_time as creatime
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.htimer as htimer
import market_data.abstract_market_data as mdabmada

_LOG = logging.getLogger(__name__)


_LOG.verb_debug = hprint.install_log_verb_debug(_LOG, verbose=False)

# #############################################################################
# ReplayedMarketData
# #############################################################################


# TODO(gp): This should have a delay and / or we should use timestamp_db.
class ReplayedMarketData(mdabmada.AbstractMarketData):
    """
    Implement an interface to a replayed time historical / RT database.

    Another approach to achieve the same goal is to mock the IM directly
    instead of this class.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        knowledge_datetime_col_name: str,
        delay_in_secs: int,
        # Params from `AbstractMarketData`.
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
        # TODO(gp): We should use the better invariant that the data is already
        #  formatted before it's saved instead of reapplying the transformation
        #  to make it palatable to downstream.
        if self._end_time_col_name in df.columns:
            hdbg.dassert_is_subset(
                [
                    self._asset_id_col,
                    self._start_time_col_name,
                    self._end_time_col_name,
                    self._knowledge_datetime_col_name,
                ],
                df.columns,
            )
            self._df.sort_values(
                [self._end_time_col_name, self._asset_id_col], inplace=True
            )

    def should_be_online(self, wall_clock_time: pd.Timestamp) -> bool:
        return True

    # TODO(gp): Remove this.
    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        _LOG.verb_debug("")
        # Sort in increasing time order and reindex.
        df = super()._normalize_data(df)
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
        _LOG.verb_debug(
            hprint.to_str(
                "start_ts end_ts ts_col_name asset_ids left_close "
                "right_close normalize_data limit"
            )
        )
        if asset_ids is not None:
            # Make sure that the requested asset_ids are in the df at some point.
            # This avoids mistakes when mocking data for certain assets, but request
            # data for assets that don't exist, which can make us wait for data that
            # will never come.
            hdbg.dassert_is_subset(
                asset_ids, self._df[self._asset_id_col].unique()
            )
        # Filter the data by the current time.
        wall_clock_time = self.get_wall_clock_time()
        _LOG.verb_debug(hprint.to_str("wall_clock_time"))
        df_tmp = creatime.get_data_as_of_datetime(
            self._df,
            self._knowledge_datetime_col_name,
            wall_clock_time,
            delay_in_secs=self._delay_in_secs,
        )
        # Handle `columns`.
        if self._columns is not None:
            hdbg.dassert_is_subset(self._columns, df_tmp.columns)
            df_tmp = df_tmp[self._columns]
        # Handle `period`.
        hdbg.dassert_in(ts_col_name, df_tmp.columns)
        df_tmp = hpandas.trim_df(
            df_tmp, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        # Handle `asset_ids`
        _LOG.verb_debug("before df_tmp=\n%s", hpandas.dataframe_to_str(df_tmp))
        if asset_ids is not None:
            hdbg.dassert_in(self._asset_id_col, df_tmp.columns)
            mask = df_tmp[self._asset_id_col].isin(set(asset_ids))
            df_tmp = df_tmp[mask]
        _LOG.verb_debug("after df_tmp=\n%s", hpandas.dataframe_to_str(df_tmp))
        # Handle `limit`.
        if limit:
            hdbg.dassert_lte(1, limit)
            df_tmp = df_tmp.head(limit)
        # Normalize data.
        if normalize_data:
            df_tmp = self._normalize_data(df_tmp)
        _LOG.verb_debug("-> df_tmp=\n%s", hpandas.dataframe_to_str(df_tmp))
        return df_tmp

    def _get_last_end_time(self) -> Optional[pd.Timestamp]:
        # We need to find the last timestamp before the current time. We use
        # `7W` but could also use all the data since we don't call the DB.
        # TODO(gp): SELECT MAX(start_time) instead of getting all the data
        #  and then find the max and use `start_time`
        timedelta = pd.Timedelta("7D")
        df = self.get_data_for_last_period(timedelta)
        _LOG.debug(hpandas.df_to_short_str("after get_data", df))
        if df.empty:
            ret = None
        else:
            ret = df.index.max()
        _LOG.debug("-> ret=%s", ret)
        return ret


# #############################################################################
# Serialize / deserialize example of DB.
# #############################################################################


def save_market_data(
    market_data: mdabmada.AbstractMarketData,
    file_name: str,
    timedelta: pd.Timedelta,
    limit: Optional[int],
) -> None:
    """
    Save data without normalization from a `MarketData` to a CSV file.

    Data is saved without normalization since we want to save it in the same format
    that a derived class is delivering it to the

    E.g.,
    ```
                start_time             end_time   egid   close   volume          timestamp_db
    0  2021-12-31 20:41:00  2021-12-31 20:42:00  16878  294.81   70273    2021-12-31 20:42:06
    1  2021-12-31 20:41:00  2021-12-31 20:42:00  16187  398.89   115650   2021-12-31 20:42:06
    2  2021-12-31 20:41:00  2021-12-31 20:42:00  15794  3345.04  6331     2021-12-31 20:42:06
    3  2021-12-31 20:41:00  2021-12-31 20:42:00  14592  337.75   26750    2021-12-31 20:42:06
    ```
    """
    hdbg.dassert(market_data.is_online())
    normalize_data = False
    with htimer.TimedScope(logging.DEBUG, "market_data.get_data"):
        rt_df = market_data.get_data_for_last_period(
            timedelta, normalize_data=normalize_data, limit=limit
        )
    _LOG.debug(hpandas.df_to_short_str("rt_df", rt_df, print_dtypes=True))
    #
    _LOG.info("Saving ...")
    compression = None
    if file_name.endswith(".gz"):
        compression = "gzip"
    rt_df.to_csv(file_name, compression=compression, index=True)
    _LOG.info("Saving done")


def load_market_data(
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
    kwargs_tmp = {}
    if aws_profile:
        s3fs_ = hs3.get_s3fs(aws_profile)
        kwargs_tmp["s3fs"] = s3fs_
    kwargs.update(kwargs_tmp)  # type: ignore[arg-type]
    df = cpanh.read_csv(file_name, **kwargs)
    # TODO(gp): The data needs to be saved after the normalization so that
    #  it's in the right format to be replayed.
    df.reset_index(inplace=True)
    _LOG.debug(hpandas.df_to_short_str("df", df, print_dtypes=True))
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
    from IPython.display import display

    display(df.head(3))
    display(df.tail(3))
