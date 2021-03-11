import datetime
import logging
import os
from typing import Any, Iterator, List, Optional, Tuple, Union

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")

import pandas as pd

# from tqdm.notebook import tqdm
from tqdm import tqdm

# import core.explore as cexplo
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.list as hlist
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


def ib_connect(client_id: int = 0, is_notebook: bool = True) -> ib_insync.ib.IB:
    # TODO(gp): Add check if we are in notebook.
    if is_notebook:
        ib_insync.util.startLoop()
    ib = ib_insync.IB()
    host = os.environ["IB_GW_CONNECTION_HOST"]
    port = os.environ["IB_GW_CONNECTION_PORT"]
    _LOG.debug("Trying to connect to client_id=%s", client_id)
    ib.connect(host=host, port=port, clientId=client_id)
    #
    ib_insync.IB.RaiseRequestErrors = True
    _LOG.debug("Connected to IB: client_id=%s", client_id)
    return ib


def to_contract_details(ib, contract):
    print("contract= (%s)\n\t%s" % (type(contract), contract))
    contract_details = ib.reqContractDetails(contract)
    print(
        "contract_details= (%s)\n\t%s"
        % (type(contract_details), contract_details)
    )
    dbg.dassert_eq(len(contract_details), 1)
    return hprint.obj_to_str(contract_details[0])


def get_contract_details(
    ib: ib_insync.ib.IB, contract: ib_insync.Contract, simplify_df: bool = False
) -> pd.DataFrame:
    _LOG.debug("contract=%s", contract)
    cds = ib.reqContractDetails(contract)
    _LOG.info("num contracts=%s", len(cds))
    contracts = [cd.contract for cd in cds]
    _LOG.debug("contracts[0]=%s", contracts[0])
    contracts_df = ib_insync.util.df(contracts)
    if simplify_df:
        # TODO(*): remove or avoid since it is only one place where `core` is used.
        # _LOG.debug(cexplo.print_column_variability(contracts_df))
        # Remove exchange.
        _LOG.debug("exchange=%s", contracts_df["exchange"].unique())
        contracts_df.sort_values("lastTradeDateOrContractMonth", inplace=True)
        contracts_df = contracts_df.drop(columns=["exchange", "comboLegs"])
        # Remove duplicates.
        contracts_df = contracts_df.drop_duplicates()
        # Remove constant values.
        # threshold = 1
        # TODO(*): remove or avoid since it is only one place where `core` is used.
        # contracts_df = cexplo.remove_columns_with_low_variability(
        #     contracts_df, threshold
        # )
    return contracts_df


# #############################################################################


def get_df_signature(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""
    txt = "len=%d [%s, %s]" % (len(df), df.index[0], df.index[-1])
    return txt


def to_ET(
    ts: Union[datetime.datetime, pd.Timestamp, str], as_datetime: bool = True
) -> Union[datetime.datetime, pd.Timestamp, str]:
    # Handle IB convention that an empty string means now.
    if ts == "":
        return ""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(tz="America/New_York")
    else:
        ts = ts.tz_convert(tz="America/New_York")
    if as_datetime:
        ts = ts.to_pydatetime()
    return ts


def to_timestamp_str(ts: pd.Timestamp) -> str:
    dbg.dassert_is_not(ts, None)
    return ts.strftime("%Y%m%dT%H%M%S")


# #############################################################################


async def req_historical_data_async(
    ib,
    contract,
    end_ts,
    duration_str,
    bar_size_setting,
    what_to_show,
    use_rth,
    num_retry=None,
):
    """
    Wrap ib.reqHistoricalData() adding a retry semantic and returning a df.

    IB seem to align days on boundaries at 18:00 of every day.
    """
    num_retry = 3 or num_retry
    end_ts = to_ET(end_ts)
    #
    for i in range(num_retry):
        bars = []
        try:
            # bars = ib.reqHistoricalData(
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_ts,
                durationStr=duration_str,
                barSizeSetting=bar_size_setting,
                whatToShow=what_to_show,
                useRTH=use_rth,
                # Use UTC.
                formatDate=2,
            )
            break
        except ib_insync.wrapper.RequestError as e:
            _LOG.warning(str(e))
            if e.code == 162:
                # RequestError: API error: 162: Historical Market Data Service
                #   error message:HMDS query returned no data
                # There is no data.
                break
            else:
                # Retry.
                _LOG.info("Retry: %s / %s", i + 1, num_retry)
                if i == num_retry:
                    dbg.dfatal("Failed after %s retries", num_retry)
    if bars:
        # Sanity check.
        dbg.dassert_lte(bars[0].date, bars[-1].date)
        # Organize the data as a dataframe with increasing times.
        df = ib_insync.util.df(bars)
        df.set_index("date", drop=True, inplace=True)
        dbg.dassert_monotonic_index(df)
        # Convert to ET.
        df.index = df.index.tz_convert(tz="America/New_York")
        _LOG.debug("df=%s", get_df_signature(df))
    else:
        df = pd.DataFrame()
    return df


def req_historical_data(
    ib: ib_insync.ib.IB,
    contract: ib_insync.Contract,
    end_ts: Union[datetime.datetime, pd.Timestamp, str],
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    num_retry: Optional[int] = None,
) -> pd.DataFrame:
    """
    Wrap ib.reqHistoricalData() adding a retry semantic and returning a df.

    IB seem to align days on boundaries at 18:00 of every day.
    """
    _check_ib_connected(ib)
    num_retry = 3 or num_retry
    end_ts = to_ET(end_ts)
    #
    for i in range(num_retry):
        bars = []
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_ts,
                durationStr=duration_str,
                barSizeSetting=bar_size_setting,
                whatToShow=what_to_show,
                useRTH=use_rth,
                # Use UTC.
                formatDate=2,
            )
            break
        except ib_insync.wrapper.RequestError as e:
            _LOG.warning(str(e))
            if e.code == 162:
                # RequestError: API error: 162: Historical Market Data Service
                #   error message:HMDS query returned no data
                # There is no data.
                break
            else:
                # Retry.
                _LOG.info("Retry: %s / %s", i + 1, num_retry)
                if i == num_retry:
                    dbg.dfatal("Failed after %s retries", num_retry)
    if bars:
        # Sanity check.
        dbg.dassert_lte(bars[0].date, bars[-1].date)
        # Organize the data as a dataframe with increasing times.
        df = ib_insync.util.df(bars)
        df.set_index("date", drop=True, inplace=True)
        dbg.dassert_monotonic_index(df)
        # Convert to ET.
        df.index = df.index.tz_convert(tz="America/New_York")
        _LOG.debug("df=%s", get_df_signature(df))
    else:
        df = pd.DataFrame()
    return df


def get_end_timestamp(
    ib, contract, what_to_show, use_rth, num_retry=None
) -> datetime.datetime:
    """
    Return the last available timestamp by querying the historical data.
    """
    endDateTime = ""
    duration_str = "1 D"
    bar_size_setting = "1 min"
    bars = req_historical_data(
        ib,
        contract,
        endDateTime,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        num_retry=num_retry,
    )
    dbg.dassert(not bars.empty)
    # Get the last timestamp.
    last_ts = bars.index[-1]
    return last_ts


# #############################################################################


def duration_str_to_pd_dateoffset(duration_str: str) -> pd.DateOffset:
    if duration_str == "2 D":
        ret = pd.DateOffset(days=2)
    elif duration_str == "7 D":
        ret = pd.DateOffset(days=7)
    elif duration_str == "1 D":
        ret = pd.DateOffset(days=1)
    else:
        raise ValueError("Invalid duration_str='%s'" % duration_str)
    return ret


def ib_loop_generator(
    ib: ib_insync.ib.IB,
    contract: ib_insync.Contract,
    start_ts: datetime,
    end_ts: datetime,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    use_progress_bar: bool = False,
    num_retry: Optional[Any] = None,
) -> Iterator[
    Union[
        Iterator,
        Iterator[
            Tuple[int, pd.DataFrame, Tuple[datetime.datetime, pd.Timestamp]]
        ],
        Iterator[Tuple[int, pd.DataFrame, Tuple[pd.Timestamp, pd.Timestamp]]],
    ]
]:
    """
    Get historical data using the IB style of looping for [start_ts, end_ts).

    The IB loop style consists in starting from the end of the interval
    and then using the earliest value returned to move the window
    backwards in time. The problem with this approach is that one can't
    parallelize the requests of chunks of data.
    """
    _check_ib_connected(ib)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = to_ET(start_ts)
    end_ts = to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lt(start_ts, end_ts)
    # Let's start from the end.
    curr_ts = end_ts
    pbar = None
    i = 0
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    ts_seq = None
    while True:
        _LOG.debug("Requesting data for curr_ts='%s'", curr_ts)
        df = req_historical_data(
            ib,
            contract,
            curr_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            num_retry=num_retry,
        )
        if df is None:
            return
            # TODO(gp): Sometimes IB returns an empty df in a chunk although there is more data
            # later on. Maybe we can just keep going.
            return
        _LOG.debug("df=%s\n%s", get_df_signature(df), df.head(3))
        if df.empty:
            # Sometimes IB returns an empty df in a chunk although there is more
            # data later on: we keep going.
            date_offset = duration_str_to_pd_dateoffset(duration_str)
            curr_ts = curr_ts - date_offset
            _LOG.debug("Empty df -> curr_ts=%s", curr_ts)
            continue
        # Move the curr_ts to the beginning of the chuck.
        next_curr_ts = df.index[0]
        ts_seq = (curr_ts, next_curr_ts)
        curr_ts = next_curr_ts
        _LOG.debug("curr_ts='%s'", curr_ts)
        if i == 0:
            # Create the progress bar.
            total = (end_ts - start_ts).days
            if use_progress_bar:
                pbar = tqdm(total=total, desc=contract.symbol)
        if pbar is not None:
            idx = (end_ts - curr_ts).days
            _LOG.debug("idx=%s, total=%s", idx, pbar.total)
            pbar.n = idx
            pbar.refresh()
        yield i, df, ts_seq
        # We insert at the beginning since we are walking backwards the interval.
        if start_ts != "" and curr_ts <= start_ts:
            _LOG.debug(
                "Reached the beginning of the interval: "
                "curr_ts=%s start_ts=%s",
                curr_ts,
                start_ts,
            )
            return
        i += 1


def _process_start_end_ts(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> Tuple[datetime.datetime, datetime.datetime]:
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = to_ET(start_ts)
    end_ts = to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lte(start_ts, end_ts)
    return start_ts, end_ts


def _truncate(
    df: pd.DataFrame, start_ts: datetime, end_ts: datetime
) -> pd.DataFrame:
    _LOG.debug("Before truncation: df=%s", get_df_signature(df))
    _LOG.debug("df.head=\n%s\ndf.tail=\n%s", df.head(3), df.tail(3))
    dbg.dassert_isinstance(df.index[0], pd.Timestamp)
    dbg.dassert_monotonic_index(df)
    start_ts = pd.Timestamp(start_ts)
    end_ts = pd.Timestamp(end_ts)
    end_ts3 = end_ts - pd.DateOffset(seconds=1)
    _LOG.debug("start_ts= %s end_ts3=%s", start_ts, end_ts3)
    df = df[start_ts:end_ts3]
    _LOG.debug("After truncation: df=%s", get_df_signature(df))
    return df


def _check_ib_connected(ib: ib_insync.ib.IB) -> None:
    # Too chatty but useful for debug.
    # _LOG.debug("ib=%s", ib)
    dbg.dassert_isinstance(ib, ib_insync.ib.IB)
    dbg.dassert(ib.isConnected())


def _allocate_ib(ib: Union[ib_insync.ib.IB, int]) -> Tuple[ib_insync.IB, bool]:
    if isinstance(ib, int):
        client_id = ib
        ib = ib_connect(client_id, is_notebook=False)
        deallocate_ib = True
    elif isinstance(ib, ib_insync.ib.IB):
        deallocate_ib = False
    else:
        raise ValueError("Invalid ib=%s of type='%s'", ib, type(ib))
    _check_ib_connected(ib)
    return ib, deallocate_ib


def _deallocate_ib(ib: ib_insync.ib.IB, deallocate_ib: bool) -> None:
    if deallocate_ib:
        _check_ib_connected(ib)
        ib.disconnect()


def get_historical_data_with_IB_loop(
    ib: ib_insync.ib.IB,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    use_progress_bar: bool = False,
    return_ts_seq: bool = False,
    num_retry: Optional[Any] = None,
) -> Tuple[
    pd.DataFrame,
    List[Tuple[Union[datetime.datetime, pd.Timestamp], pd.Timestamp]],
]:
    """
    Get historical data using the IB style of looping for [start_ts, end_ts).

    The IB loop style consists in starting from the end of the interval
    and then using the earliest value returned to move the window
    backwards in time. The problem with this approach is that one can't
    parallelize the requests of chunks of data.
    """
    start_ts, end_ts = _process_start_end_ts(start_ts, end_ts)
    #
    dfs = []
    ts_seq = []
    ib, deallocate_ib = _allocate_ib(ib)
    generator = ib_loop_generator(
        ib,
        contract,
        start_ts,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        use_progress_bar=use_progress_bar,
        num_retry=num_retry,
    )
    # Deallocate.
    _deallocate_ib(ib, deallocate_ib)
    for i, df_tmp, ts_seq_tmp in generator:
        ts_seq.append(ts_seq_tmp)
        dfs.insert(0, df_tmp)
    #
    df = pd.concat(dfs)
    df = _truncate(df, start_ts, end_ts)
    if return_ts_seq:
        return df, ts_seq
    return df


def save_historical_data_with_IB_loop(
    ib: ib_insync.ib.IB,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    file_name: str,
    use_progress_bar: bool = False,
    num_retry: Optional[Any] = None,
) -> pd.DataFrame:
    """
    Like get_historical_data_with_IB_loop but saving on a file.
    """
    # TODO(gp): Factor this out.
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = to_ET(start_ts)
    end_ts = to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lt(start_ts, end_ts)
    #
    generator = ib_loop_generator(
        ib,
        contract,
        start_ts,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        use_progress_bar=use_progress_bar,
        num_retry=num_retry,
    )
    for i, df_tmp, ts_seq_tmp in generator:
        # Update file.
        header = i == 0
        mode = "w" if header else "a"
        df_tmp.to_csv(file_name, mode=mode, header=header)
    #
    _LOG.debug("Reading back %s", file_name)
    df = load_historical_data(file_name)
    # It is not sorted since we are going back.
    df = df.sort_index()
    #
    df = _truncate(df, start_ts, end_ts)
    #
    df.to_csv(file_name, mode="w")
    return df


def historical_data_to_filename(
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    dst_dir: str,
) -> str:
    # Create the filename.
    symbol = contract.symbol
    bar_size_setting = bar_size_setting.replace(" ", "_")
    duration_str = duration_str.replace(" ", "_")
    file_name = f"{symbol}.{to_timestamp_str(start_ts)}.{to_timestamp_str(end_ts)}.{duration_str}.{bar_size_setting}.{what_to_show}.{use_rth}.csv"
    file_name = os.path.join(dst_dir, file_name)
    return file_name


def save_historical_data_single_file_with_IB_loop(
    ib: int,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    file_name: str,
    incremental: bool,
    use_progress_bar: bool = True,
    num_retry: Optional[Any] = None,
) -> None:
    """
    Save historical data into a single `file_name`.

    :param incremental: if the file already exists, resume downloading from the last
        date
    """
    if incremental and os.path.exists(file_name):
        df = load_historical_data(file_name)
        end_ts = df.index.min()
        _LOG.warning(
            "Found file '%s': starting from end_ts=%s because incremental mode",
            file_name,
            end_ts,
        )
    #
    start_ts, end_ts = _process_start_end_ts(start_ts, end_ts)
    #
    ib, deallocate_ib = _allocate_ib(ib)
    _LOG.debug("ib=%s", ib)
    generator = ib_loop_generator(
        ib,
        contract,
        start_ts,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        use_progress_bar=use_progress_bar,
        num_retry=num_retry,
    )
    for i, df_tmp, _ in generator:
        # Update file.
        is_first_iter = i == 0
        if is_first_iter:
            if incremental:
                # In incremental mode, we always append.
                mode = "a"
                header = False
            else:
                # In non-incremental mode, only the first iteration requires to
                # write the header.
                mode = "w"
                header = True
        else:
            # If it's not the first iteration, we need to append.
            mode = "a"
            header = False
        df_tmp.to_csv(file_name, mode=mode, header=header)
    _deallocate_ib(ib, deallocate_ib)
    # Load everything and clean it up.
    df = load_historical_data(file_name)
    df.sort_index(inplace=True)
    df = _truncate(df, start_ts, end_ts)
    _LOG.info("Saved full data in '%s'", file_name)
    df.to_csv(file_name)


# def save_historical_data_with_IB_loop(ib, contract, start_ts, end_ts, duration_str,
#                                       bar_size_setting,
#                                       what_to_show, use_rth,
#                                       dst_dir,
#                                       incremental,
#                                       use_progress_bar=True,
#                                       num_retry=None):
#     """
#     Save historical data into `file_name`.
#
#     :param incremental: if the file already exists, resume downloading from the last
#         date
#     """
#     start_ts, end_ts = _process_start_end_ts(start_ts, end_ts)
#     #
#     ib, deallocate_ib = _allocate_ib(ib)
#     _LOG.debug("ib=%s", ib)
#     generator = ib_loop_generator(ib, contract, start_ts, end_ts, duration_str,
#                                   bar_size_setting,
#                                   what_to_show, use_rth,
#                                   use_progress_bar=use_progress_bar,
#                                   num_retry=num_retry)
#     for i, df_tmp, _ in generator:
#         # Update file.
#         is_first_iter = i == 0
#         if is_first_iter:
#             if incremental:
#                 # In incremental mode, we always append.
#                 mode = "a"
#                 header = False
#             else:
#                 # In non-incremental mode, only the first iteration requires to
#                 # write the header.
#                 mode = "w"
#                 header = True
#         else:
#             # If it's not the first iteration, we need to append.
#             mode = "a"
#             header = False
#         df_tmp.to_csv(file_name, mode=mode, header=header)
#     _deallocate_ib(ib, deallocate_ib)
#     # Load everything and clean it up.
#     df = load_historical_data(file_name)
#     df.sort_index(inplace=True)
#     df = _truncate(df, start_ts, end_ts)
#     _LOG.info("Saved full data in '%s'", file_name)
#     df.to_csv(file_name)


def load_historical_data(file_name: str, verbose: bool = False) -> pd.DataFrame:
    """
    Load data generated by functions like save_historical_data_with_IB_loop().
    """
    _LOG.debug("file_name=%s", file_name)
    df = pd.read_csv(file_name, parse_dates=True, index_col="date")
    # dbg.dassert_isinstance(df.index[0], pd.Timestamp)
    if verbose:
        _LOG.info(
            "%s: %d [%s, %s]", file_name, df.shape[0], df.index[0], df.index[-1]
        )
        _LOG.info(df.head(2))
    return df


# #############################################################################


def select_assets(ib, target: str, frequency: str, symbol: str):
    #
    if target == "futures":
        contract = ib_insync.Future(symbol, "202109", "GLOBEX", currency="USD")
        what_to_show = "TRADES"
    if target == "continuous_futures":
        contract = ib_insync.ContFuture(symbol, "GLOBEX", currency="USD")
        what_to_show = "TRADES"
    elif target == "stocks":
        contract = ib_insync.Stock(symbol, "SMART", currency="USD")
        what_to_show = "TRADES"
    elif target == "forex":
        contract = ib_insync.Forex(symbol)
        what_to_show = "MIDPOINT"
    else:
        dbg.dfatal("Invalid target='%s'" % target)
    #
    ib.qualifyContracts(contract)
    if frequency == "intraday":
        # duration_str = '2 D'
        duration_str = "7 D"
        bar_size_setting = "1 min"
    elif frequency == "hour":
        duration_str = "2 D"
        bar_size_setting = "1 hour"
    elif frequency == "day":
        duration_str = "1 Y"
        bar_size_setting = "1 day"
    else:
        dbg.dfatal("Invalid frequency='%s'" % frequency)
    return contract, duration_str, bar_size_setting, what_to_show


def get_tasks(
    ib: ib_insync.ib.IB,
    target: str,
    frequency: str,
    symbols: List[str],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    use_rth: bool,
) -> List[
    Tuple[ib_insync.Contract, pd.Timestamp, pd.Timestamp, str, str, str, bool]
]:
    tasks = []
    for symbol in symbols:
        contract, duration_str, bar_size_setting, what_to_show = select_assets(
            ib, target, frequency, symbol
        )
        if start_ts is None:
            start_ts = ib.reqHeadTimeStamp(
                contract, whatToShow=what_to_show, useRTH=use_rth
            )
        if end_ts is None:
            end_ts = get_end_timestamp(ib, contract, what_to_show, use_rth)
            # end_ts = pd.Timestamp("2020-12-13 18:00:00-05:00")
        task = (
            contract,
            start_ts,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        )
        tasks.append(task)
    return tasks


def process_workload(
    client_id: int,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    dst_dir: str,
    incremental: bool,
) -> str:
    file_name = historical_data_to_filename(
        contract,
        start_ts,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        dst_dir,
    )
    save_historical_data_single_file_with_IB_loop(
        client_id,
        contract,
        start_ts,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
        file_name,
        incremental,
        use_progress_bar=True,
        num_retry=None,
    )
    # else:
    #     save_historical_data_with_IB_loop(client_id, contract, start_ts, end_ts,
    #                                       duration_str,
    #                                       bar_size_setting,
    #                                       what_to_show, use_rth,
    #                                       file_name,
    #                                       incremental,
    #                                       use_progress_bar=True,
    #                                       num_retry=None)
    return file_name


def download_ib_data(
    client_id_base: int,
    tasks: List[
        Tuple[ib_insync.Contract, pd.Timestamp, pd.Timestamp, str, str, str, bool]
    ],
    incremental: bool,
    dst_dir: str,
    num_threads: str,
) -> List[str]:
    _LOG.info("Tasks=%s\n%s", len(tasks), "\n".join(map(str, tasks)))
    hio.create_dir(dst_dir, incremental=True)
    # ib.reqMarketDataType(4)
    file_names = []
    if num_threads == "serial":
        for client_id, task in tqdm(enumerate(tasks), desc="download_ib_data"):
            (
                contract,
                start_ts,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
            ) = task
            file_name = process_workload(
                client_id_base + client_id,
                contract,
                start_ts,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                dst_dir,
                incremental,
            )
            file_names.append(file_name)
    else:
        num_threads = int(num_threads)
        # -1 is interpreted by joblib like for all cores.
        _LOG.info(
            "Using %s threads", num_threads if num_threads > 0 else "all CPUs"
        )
        import joblib

        file_names = joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(process_workload)(
                client_id_base + client_id,
                contract,
                start_ts,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                dst_dir,
                incremental,
            )
            for client_id, (
                contract,
                start_ts,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
            ) in enumerate(tasks)
        )
    return file_names


# #############################################################################

_SIX_PM = (18, 0, 0)


def _get_hh_mm_ss(ts: pd.Timestamp) -> Tuple[int, int, int]:
    """
    Return the hour, minute, second part of a timestamp.
    """
    # Round to seconds.
    ts = ts.round("1s")
    return ts.hour, ts.minute, ts.second


def _set_to_six_pm(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Set the hour, minute, second part of a timestamp to 18:00:00.
    """
    ts = ts.replace(hour=18, minute=0, second=0)
    return ts


def _align_to_six_pm(
    ts: pd.Timestamp, align_right: bool
) -> Tuple[pd.Timestamp, List[pd.Timestamp]]:
    """
    Align a timestamp to the previous / successive 6pm, unless it's already
    aligned.

    :param ts: timestamp to align
    :param align_right: align on right or left
    :return: return the aligned timestamp and the previous unaligned timestamp
    """
    _LOG.debug("ts='%s'", ts)
    if _get_hh_mm_ss(ts) > _SIX_PM:
        dates = [ts]
        # Align ts to 18:00.
        ts = _set_to_six_pm(ts)
    elif _get_hh_mm_ss(ts) < _SIX_PM:
        dates = [ts]
        # Align ts to 18:00 of the day before.
        ts = _set_to_six_pm(ts)
        if align_right:
            ts += pd.DateOffset(days=1)
        else:
            ts -= pd.DateOffset(days=1)
    else:
        # ts is already aligned.
        dates = [ts]
    _LOG.debug("-> ts='%s' dates=%s", ts, dates)
    return ts, dates


def _start_end_ts_to_ET(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Convert to timestamps with timezone, if needed.
    """
    dbg.dassert_lt(start_ts, end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = to_ET(start_ts, as_datetime=False)
    end_ts = to_ET(end_ts, as_datetime=False)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    return start_ts, end_ts


def _ib_date_range_sanity_check(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp, dates: List[pd.Timestamp]
) -> List[pd.Timestamp]:
    # Remove some weird pandas artifacts (e.g., 'freq=2D') related to sampling.
    dates = [pd.Timestamp(ts.to_pydatetime()) for ts in dates]
    # Remove duplicates.
    dates = hlist.remove_duplicates(dates)
    _LOG.debug("-> dates=%s", dates)
    # Sanity check.
    dbg.dassert_eq(sorted(dates), dates)
    dbg.dassert_eq(len(set(dates)), len(dates))
    # Using each date as end of intervals [date - days(2), date], we should
    # cover the entire interval [start_ts, end_ts].
    dbg.dassert_lte(dates[0] - pd.DateOffset(days=2), start_ts)
    dbg.dassert_lte(end_ts, dates[-1])
    return dates


def _ib_date_range(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> List[pd.Timestamp]:
    """
    Compute a date range covering [start_ts, end_ts] using the IB loop-style.

    The IB loop-style consists of iterating among days aligned on 6pm.
    """
    start_ts, end_ts = _start_end_ts_to_ET(start_ts, end_ts)
    # Align start_ts and end_ts to 6pm to create a date interval that includes
    # [start_ts, end_ts].
    start_ts_tmp, start_dates = _align_to_six_pm(start_ts, align_right=False)
    # Compute a range of dates aligned to 6pm that includes
    # [start_ts_tmp, end_ts_tmp].
    _LOG.debug("start_ts_tmp='%s' end_ts='%s'", start_ts_tmp, end_ts)
    dbg.dassert_eq(_get_hh_mm_ss(start_ts_tmp), _SIX_PM)
    dbg.dassert_lt(start_ts_tmp, end_ts)
    dates = pd.date_range(start=start_ts_tmp, end=end_ts, freq="2D").tolist()
    # If the first date is before start_ts, then we don't need since the interval
    # [date - days(2), date] doesn't overlap with [start_ts, end_ts].
    if dates[0] < start_ts:
        dates = dates[1:]
    # If the last date
    if dates[-1] < end_ts:
        dates.append(end_ts)
    dates = _ib_date_range_sanity_check(start_ts, end_ts, dates)
    return dates


def get_historical_data_workload(
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
) -> Tuple[
    List[Tuple[ib_insync.Contract, pd.Timestamp, str, str, str, bool]],
    List[pd.Timestamp],
]:
    """
    Compute the workload needed to get data in [start_ts, end_ts].
    """
    _LOG.debug(
        "contract='%s', start_ts='%s', end_ts='%s', bar_size_setting='%s', "
        "what_to_show='%s', use_rth='%s'",
        contract,
        start_ts,
        end_ts,
        bar_size_setting,
        what_to_show,
        use_rth,
    )
    start_ts, end_ts = _start_end_ts_to_ET(start_ts, end_ts)
    dbg.dassert_lte(3, (end_ts - start_ts).days)
    dates = _ib_date_range(start_ts, end_ts)
    # duration_str = "2 D"
    duration_str = "7 D"
    tasks = []
    for end in dates:
        task = (
            contract,
            end,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        )
        _LOG.debug("date='%s' -> task='%s'", end, task)
        tasks.append(task)
    return tasks, dates


def get_historical_data_from_tasks(
    client_id: int,
    tasks: List[Tuple[ib_insync.Contract, pd.Timestamp, str, str, str, bool]],
    use_prograss_bar: bool = False,
) -> pd.DataFrame:
    """
    Execute the workload serially.
    """
    df = []
    ib = ib_connect(client_id, is_notebook=False)
    if use_prograss_bar:
        tasks = tqdm(tasks)
    for task in tasks:
        _LOG.debug("task='%s'", task)
        (
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        ) = task
        df_tmp = req_historical_data(
            ib,
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        )
        dbg.dassert_monotonic_index(df_tmp)
        _LOG.debug("%s -> df_tmp=%s", end_ts, get_df_signature(df_tmp))
        df.append(df_tmp)
    #
    ib.disconnect()
    #
    df = pd.concat(df)
    # There can be overlap between the first 2 chunks.
    df.sort_index(inplace=True)
    df.drop_duplicates(inplace=True)
    return df


# #############################################################################


def _task_to_filename(
    contract,
    end_ts,
    duration_str,
    bar_size_setting,
    what_to_show,
    use_rth,
    dst_dir,
):
    # Create the filename.
    symbol = contract.symbol
    bar_size_setting = bar_size_setting.replace(" ", "_")
    duration_str = duration_str.replace(" ", "_")
    file_name = f"{symbol}.{to_timestamp_str(end_ts)}.{duration_str}.{bar_size_setting}.{what_to_show}.{use_rth}.csv"
    file_name = os.path.join(dst_dir, file_name)
    return file_name


def _execute_ptask(
    client_id,
    contract,
    end_ts,
    duration_str,
    bar_size_setting,
    what_to_show,
    use_rth,
    file_name,
):
    ib = ib_connect(client_id, is_notebook=False)
    df = req_historical_data(
        ib,
        contract,
        end_ts,
        duration_str,
        bar_size_setting,
        what_to_show,
        use_rth,
    )
    ib.disconnect()
    dbg.dassert_monotonic_index(df)
    _LOG.debug("%s -> df=%s", end_ts, get_df_signature(df))
    df.to_csv(file_name)


def get_historical_data_parallel(tasks, num_threads, incremental, dst_dir):
    """
    Execute the workload in parallel.
    """
    # Prepare parallel tasks.
    ptasks = []
    cnt_skip = 0
    for task in tasks:
        _LOG.debug("task='%s'", task)
        (
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
        ) = task
        file_name = _task_to_filename(
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            dst_dir,
        )
        if incremental and os.path.exists(file_name):
            _LOG.debug(
                "Found file %s: skipping corresponding workload", file_name
            )
            cnt_skip += 1
            continue
        ptask = (
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            file_name,
        )
        ptasks.append(ptask)
    _LOG.warning(
        "Found %s tasks already executed on disk: skipping",
        hprint.perc(cnt_skip, len(tasks)),
    )
    # Execute parallel workload.
    if num_threads == "serial":
        client_id = 0
        for ptask in tqdm(ptasks):
            (
                contract,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                file_name,
            ) = ptask
            _execute_ptask(
                client_id,
                contract,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                file_name,
            )
    else:
        # -1 is interpreted by joblib like for all cores.
        _LOG.info(
            "Using %s threads", num_threads if num_threads > 0 else "all CPU"
        )
        import joblib

        joblib.Parallel(n_jobs=num_threads, verbose=50)(
            joblib.delayed(_execute_ptask)(
                client_id + 1,
                contract,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                file_name,
            )
            for client_id, (
                contract,
                end_ts,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                file_name,
            ) in enumerate(ptasks)
        )
    # Load the data back.
    df = []
    for ptask in ptasks:
        _LOG.debug("ptask='%s'", ptask)
        (
            contract,
            end_ts,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            file_name,
        ) = ptask
        df_tmp = load_historical_data(file_name)
        df.append(df_tmp)
    #
    df = pd.concat(df)
    # There can be overlap between the first 2 chunks.
    df.sort_index(inplace=True)
    df.drop_duplicates(inplace=True)
    return df


# #############################################################################


def get_historical_data(
    client_id: int,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    mode: str,
    num_threads: str = "serial",
    incremental: bool = False,
    dst_dir: Optional[Any] = None,
    use_progress_bar: bool = False,
    return_ts_seq: bool = False,
) -> Tuple[pd.DataFrame, List[pd.Timestamp]]:
    _LOG.debug(
        "client_id='%s', contract='%s', end_ts='%s', duration_str='%s', bar_size_setting='%s', "
        "what_to_show='%s', use_rth='%s'",
        client_id,
        contract,
        start_ts,
        end_ts,
        bar_size_setting,
        what_to_show,
        use_rth,
    )
    start_ts, end_ts = _start_end_ts_to_ET(start_ts, end_ts)
    tasks, dates = get_historical_data_workload(
        contract, start_ts, end_ts, bar_size_setting, what_to_show, use_rth
    )
    _LOG.debug("dates=%s", dates)
    if mode == "in_memory":
        df = get_historical_data_from_tasks(
            client_id, tasks, use_prograss_bar=use_progress_bar
        )
    elif mode == "on_disk":
        df = get_historical_data_parallel(
            tasks, num_threads, incremental, dst_dir
        )
    else:
        raise ValueError("Invalid mode='%s'" % mode)
    #
    if not df.empty:
        # import helpers.unit_test as hut
        # hut.diff_df_monotonic(df)
        dbg.dassert_monotonic_index(df)
        end_ts3 = end_ts - pd.DateOffset(seconds=1)
        _LOG.debug("start_ts= %s end_ts3=%s", start_ts, end_ts3)
        df = df[start_ts:end_ts3]
    #
    if return_ts_seq:
        return df, dates
    return df
