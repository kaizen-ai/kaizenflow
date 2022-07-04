"""
Import as:

import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut
"""

import helpers.hs3 as hs3


def get_latest_data_snapshot(root_dir: str, aws_profile: str) -> str:
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
