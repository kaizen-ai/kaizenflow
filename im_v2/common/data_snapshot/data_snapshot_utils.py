"""
Import as:

import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut
"""

from typing import Optional

import helpers.hdbg as hdbg
import helpers.hs3 as hs3


def get_latest_data_snapshot(
    root_dir: str,
    aws_profile: Optional[str],
    *,
    data_snapshot: str = None,
) -> str:
    """
    Get the latest numeric data snapshot or daily updating snapshot if `latest`
    is given.
    """
    if data_snapshot is None:
        pattern = "*"
        only_files = False
        use_relatives_paths = True
        dirs = hs3.listdir(
            root_dir,
            pattern,
            only_files,
            use_relatives_paths,
            aws_profile=aws_profile,
        )
        dirs = [snapshot for snapshot in dirs if snapshot.isnumeric()]
        hdbg.dassert_lte(1, len(dirs))
        data_snapshot = max(dirs)
    elif data_snapshot == "latest":
        data_snapshot = "binance"
    return data_snapshot


def dassert_is_valid_data_snapshot(data_snapshot: str) -> None:
    """
    Check if data snapshot is valid.
    """
    hdbg.dassert(data_snapshot.isnumeric())
    hdbg.dassert_eq(len(data_snapshot), 8)
