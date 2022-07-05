"""
Import as:

import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut
"""

from typing import Optional

import helpers.hdbg as hdbg
import helpers.hs3 as hs3


def get_latest_data_snapshot(root_dir: str, aws_profile: Optional[str]) -> str:
    """
    Get the latest numeric data snapshot.
    """
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
    data_snapshot = max(dirs)
    return data_snapshot


def is_valid_data_snapshot(data_snapshot: str) -> None:
    """
    Check if data snapshot valid.
    """
    hdbg.dassert_eq(len(data_snapshot), 8)
    hdbg.dassert_is(data_snapshot.isnumeric(), True)
