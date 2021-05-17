import io
import logging

import pandas as pd

_LOG = logging.getLogger(__name__)


def get_df_info_as_string(
    df: pd.DataFrame, exclude_memory_usage: bool = True
) -> str:
    """
    Get dataframe info as string.

    :param df: dataframe
    :param exclude_memory_usage: whether to exclude memory usage information
    :return: dataframe info as `str`
    """
    buffer = io.StringIO()
    df.info(buf=buffer)
    info = buffer.getvalue()
    if exclude_memory_usage:
        # Remove memory usage (and a newline).
        info = info.rsplit("\n", maxsplit=2)[0]
    return info
