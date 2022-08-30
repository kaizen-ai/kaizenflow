"""
Import as:

import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut
"""

from typing import Optional

import helpers.hdbg as hdbg
import helpers.hs3 as hs3


def get_data_snapshot(
    root_dir: str,
    aws_profile: Optional[str],
    data_snapshot: str,
) -> str:
    """
    Get data snapshot:

    - latest: return the latest numeric snapshot, e.g. "20220828"
    - updated_daily: return the snapshot that is updated continuously
    """
    if data_snapshot == "latest":
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
    dassert_is_valid_data_snapshot(data_snapshot)
    return data_snapshot


def dassert_is_valid_data_snapshot(data_snapshot: str) -> None:
    """
    Check if data snapshot is valid.
    """
    if not data_snapshot.isnumeric():
        hdbg.dassert_eq(data_snapshot, "")
    hdbg.dassert(data_snapshot.isnumeric())
    hdbg.dassert_eq(len(data_snapshot), 8)
