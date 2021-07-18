"""
Download data from IB using the "IB loop" approach (ie starting from the end of
the interval and moving backwards).
"""
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

import helpers.dbg as dbg
import helpers.s3 as hs3
import helpers.io_ as hio
import im.ib.data.extract.gateway.utils as videgu

_LOG = logging.getLogger(__name__)


# TODO(*): -> _ib_loop_generator?
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
    videgu.check_ib_connected(ib)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    start_ts = videgu.to_ET(start_ts)
    end_ts = videgu.to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lt(start_ts, end_ts)
    # Let's start from the end.
    curr_ts = end_ts
    pbar = None
    i = 0
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    ts_seq = None
    start_ts_reached = False
    while not start_ts_reached:
        _LOG.debug("Requesting data for curr_ts='%s'", curr_ts)
        df = videgu.req_historical_data(
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
            # TODO(gp): Sometimes IB returns an empty df in a chunk although there
            #  is more data later on. Maybe we can just keep going.
            return
        _LOG.debug("df=%s\n%s", videgu.get_df_signature(df), df.head(3))
        date_offset = videgu.duration_str_to_pd_dateoffset(duration_str)
        if df.empty:
            # Sometimes IB returns an empty df in a chunk although there is more
            # data later on: we keep going.
            next_curr_ts = curr_ts - date_offset
            _LOG.debug("Empty df -> curr_ts=%s", curr_ts)
        else:
            # Move the curr_ts to the beginning of the chuck.
            next_curr_ts = videgu.to_ET(df.index[0])
            # Avoid infinite loop if there is only one record in response.
            if next_curr_ts == curr_ts:
                next_curr_ts -= date_offset
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
        # We insert at the beginning since we are walking backwards the interval.
        if start_ts != "" and curr_ts <= start_ts:
            _LOG.debug(
                "Reached the beginning of the interval: "
                "curr_ts=%s start_ts=%s",
                curr_ts,
                start_ts,
            )
            df = videgu.truncate(df, start_ts=start_ts, end_ts=end_ts)
            start_ts_reached = True
        if not df.empty:
            yield i, df, ts_seq
        i += 1


def save_historical_data_by_intervals_IB_loop(
    ib: int,
    contract: ib_insync.Contract,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    duration_str: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool,
    file_name: str,
    part_files_dir: str,
    incremental: bool,
    use_progress_bar: bool = True,
    num_retry: Optional[Any] = None,
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Save historical data into multiple files into `contract.symbol` directory
    near the `file_name`.

    :param incremental: if the `file_name` already exists, resume downloading
        from the last date
    """
    start_ts, end_ts = videgu.process_start_end_ts(start_ts, end_ts)
    #
    ib, deallocate_ib = videgu.allocate_ib(ib)
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
    saved_intervals = set()
    for i, df_tmp, _ in generator:
        # Split data by static intervals.
        for interval, df_tmp_part in videgu.split_data_by_intervals(
            df_tmp, videgu.duration_str_to_pd_dateoffset(duration_str)
        ):
            # Get file name for each part.
            file_name_for_part = historical_data_to_filename(
                contract=contract,
                start_ts=interval[0],
                end_ts=interval[1],
                duration_str=duration_str,
                bar_size_setting=bar_size_setting,
                what_to_show=what_to_show,
                use_rth=use_rth,
                dst_dir=part_files_dir,
            )
            # There can be already data from previous loop iteration.
            if videgu.check_file_exists(file_name_for_part):
                df_to_write = pd.concat(
                    [df_tmp_part, load_historical_data(file_name_for_part)]
                )
            else:
                # First iteration ever.
                df_to_write = df_tmp_part
            # Force to have index `pd.Timestamp` format.
            df_to_write.index = df_to_write.index.map(videgu.to_ET)
            if incremental:
                # It is possible that same data was already loaded.
                df_to_write = df_to_write[
                    ~df_to_write.index.duplicated(keep="last")
                ]
                df_to_write.sort_index(inplace=True)
            dbg.dassert_monotonic_index(
                df_to_write,
                "Most likely the data for selected interval already exists, try incremental mode.",
            )
            # We appended data at step before, so re-write the file.
            df_to_write.to_csv(file_name_for_part, mode="w", header=True)
            _LOG.info("Saved partial data in '%s'", file_name_for_part)
            saved_intervals.add(interval)
    videgu.deallocate_ib(ib, deallocate_ib)
    return saved_intervals


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
    start_ts, end_ts = videgu.process_start_end_ts(start_ts, end_ts)
    #
    dfs = []
    ts_seq = []
    ib, deallocate_ib = videgu.allocate_ib(ib)
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
    videgu.deallocate_ib(ib, deallocate_ib)
    for i, df_tmp, ts_seq_tmp in generator:
        ts_seq.append(ts_seq_tmp)
        dfs.insert(0, df_tmp)
    #
    df = pd.concat(dfs)
    df = videgu.truncate(df, start_ts, end_ts)
    if return_ts_seq:
        return df, ts_seq
    return df


# TODO: -> _historical_data_to_filename
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
    file_name = f"{symbol}.{videgu.to_timestamp_str(start_ts)}.{videgu.to_timestamp_str(end_ts)}.{duration_str}.{bar_size_setting}.{what_to_show}.{use_rth}.csv"
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
    start_ts, end_ts = videgu.process_start_end_ts(start_ts, end_ts)
    #
    ib, deallocate_ib = videgu.allocate_ib(ib)
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
    videgu.deallocate_ib(ib, deallocate_ib)
    # Load everything and clean it up.
    df = load_historical_data(file_name)
    df.sort_index(inplace=True)
    df = videgu.truncate(df, start_ts, end_ts)
    _LOG.info("Saved full data in '%s'", file_name)
    df.to_csv(file_name)


def load_historical_data(file_name: str, verbose: bool = False) -> pd.DataFrame:
    """
    Load data generated by functions like
    `save_historical_data_with_IB_loop()`.
    """
    _LOG.debug("file_name=%s", file_name)
    s3fs = hs3.get_s3fs("am")
    df = pdhelp.read_csv(
        file_name, s3fs=s3fs, parse_dates=True, index_col=0
    )
    # dbg.dassert_isinstance(df.index[0], pd.Timestamp)
    if verbose:
        _LOG.info(
            "%s: %d [%s, %s]", file_name, df.shape[0], df.index[0], df.index[-1]
        )
        _LOG.info(df.head(2))
    return df


# #############################################################################

# TODO(*): -> _process_workload().
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
    return file_name


def download_ib_data(
    client_id_base: int,
    tasks: List[
        Tuple[ib_insync.Contract, pd.Timestamp, pd.Timestamp, str, str, str, bool]
    ],
    incremental: bool,
    dst_dir: str,
    num_threads: Union[str, int],
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
