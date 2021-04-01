import logging
from typing import Any, Optional

try:
    import ib_insync
except ModuleNotFoundError:
    print("Can't find ib_insync")

import pandas as pd

# import core.explore as cexplo
import helpers.dbg as dbg
import helpers.io_ as hio
import instrument_master.ib.data.extract.gateway.download_data_ib_loop as videgd
import instrument_master.ib.data.extract.gateway.utils as videgu

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
    start_ts = videgu.to_ET(start_ts)
    end_ts = videgu.to_ET(end_ts)
    _LOG.debug("start_ts='%s' end_ts='%s'", start_ts, end_ts)
    dbg.dassert_lt(start_ts, end_ts)
    #
    generator = videgd.ib_loop_generator(
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
    tmp_files = []
    tmp_file_pattern = "%s_part_%s"
    for i, df_tmp, ts_seq_tmp in generator:
        # Create a separate file for each chunk.
        tmp_file_name = tmp_file_pattern % (file_name, i)
        tmp_files.append(tmp_file_name)
        df_tmp.to_csv(tmp_file_name, mode="w", header=True)
    #
    _LOG.debug("Reading back %s", tmp_files)
    # Go in reverse order to make timestamps ascending.
    df = pd.concat(
        [
            videgd.load_historical_data(tmp_file_pattern % (file_name, i))
            for i in range(len(tmp_files))[::-1]
        ]
    )
    #
    df = videgu.truncate(df, start_ts, end_ts)
    #
    df.to_csv(file_name, mode="w")
    # Clean temporary files.
    for tmp_file_name in tmp_files:
        hio.delete_file(tmp_file_name)
    return df
