import logging
from typing import Any, Optional

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")

import pandas as pd

# import core.explore as cexplo
import helpers.dbg as dbg
import vendors_amp.ib.extract.download_data_ib_loop as viedow
import vendors_amp.ib.extract.utils as vieuti

# from tqdm.notebook import tqdm


_LOG = logging.getLogger(__name__)


# TODO(plyq): it should save each chunk to a separate file - after concat them back.
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
    start_ts = vieuti.to_ET(start_ts)
    end_ts = vieuti.to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lt(start_ts, end_ts)
    #
    generator = viedow.ib_loop_generator(
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
    df = viedow.load_historical_data(file_name)
    # It is not sorted since we are going back.
    df = df.sort_index()
    #
    df = vieuti.truncate(df, start_ts, end_ts)
    #
    df.to_csv(file_name, mode="w")
    return df
