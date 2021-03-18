import datetime
import logging
import os
from typing import List, Optional, Tuple, Union

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")

import pandas as pd

# import core.explore as cexplo
import helpers.dbg as dbg
import helpers.printing as hprint

# from tqdm.notebook import tqdm


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
    check_ib_connected(ib)
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
    elif duration_str == "4 D":
        ret = pd.DateOffset(days=4)
    elif duration_str == "3 D":
        ret = pd.DateOffset(days=3)
    elif duration_str == "1 M":
        ret = pd.DateOffset(months=1)
    else:
        raise ValueError("Invalid duration_str='%s'" % duration_str)
    return ret


def process_start_end_ts(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp
) -> Tuple[datetime.datetime, datetime.datetime]:
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = to_ET(start_ts)
    end_ts = to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lte(start_ts, end_ts)
    return start_ts, end_ts


def truncate(
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


def check_ib_connected(ib: ib_insync.ib.IB) -> None:
    # Too chatty but useful for debug.
    # _LOG.debug("ib=%s", ib)
    dbg.dassert_isinstance(ib, ib_insync.ib.IB)
    dbg.dassert(ib.isConnected())


def allocate_ib(ib: Union[ib_insync.ib.IB, int]) -> Tuple[ib_insync.IB, bool]:
    if isinstance(ib, int):
        client_id = ib
        ib = ib_connect(client_id, is_notebook=False)
        deallocate_ib = True
    elif isinstance(ib, ib_insync.ib.IB):
        deallocate_ib = False
    else:
        raise ValueError("Invalid ib=%s of type='%s'", ib, type(ib))
    check_ib_connected(ib)
    return ib, deallocate_ib


def deallocate_ib(ib: ib_insync.ib.IB, deallocate_ib: bool) -> None:
    if deallocate_ib:
        check_ib_connected(ib)
        ib.disconnect()


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
