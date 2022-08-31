"""
Import as:

import im_v2.common.data_snapshot.data_snapshot_utils as imvcdsdsut
"""

import re
from typing import Optional

import helpers.hdbg as hdbg
import helpers.hs3 as hs3

# Fixed data snapshots (e.g., `2022...
FIXED_DATA_SNAPSHOTS_ROOT_DIR = (
    "s3://cryptokaizen-data/reorg/historical.manual.pq"
)
# Contain `latest` data snapshot that is updated daily by Airflow.
DAILY_DATA_SNAPSHOT_ROOT_DIR = (
    "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq"
)
UNIT_TEST_DATA_DIR = "s3://cryptokaizen-data/unit_test/historical.manual.pq"


def get_data_snapshot(
    root_dir: str,
    data_snapshot: str,
    aws_profile: Optional[str],
) -> str:
    """
    Get data snapshot:

    - Historical data -- latest snapshot

        E.g.:
        ```
        root_dir = s3://cryptokaizen-data/reorg/historical.manual.pq
        data_snapshot = "latest"
        im_client = ImClient(root_dir, ..., data_snapshot, ...)
        ```
    - Historical data -- numeric snapshot

         E.g.:
         ```
         root_dir = s3://cryptokaizen-data/reorg/historical.manual.pq
         data_snapshot = "20220508"
         im_client = ImClient(root_dir, ..., data_snapshot, ...)
         ```
    - Daily updated data

         E.g.:
         ```
         root_dir = s3://cryptokaizen-data/reorg/daily_staged.airflow.pq
         data_snapshot = ""
         im_client = ImClient(root_dir, ..., data_snapshot, ...)
         ```
    """
    _dassert_is_valid_aws_profile_and_root_dir(aws_profile, root_dir)
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
        dirs = [snapshot for snapshot in dirs if re.match(r"\d{8}", snapshot)]
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
    else:
        hdbg.dassert(data_snapshot.isnumeric())
        hdbg.dassert_eq(len(data_snapshot), 8)


def _dassert_is_valid_aws_profile_and_root_dir(
    aws_profile: Optional[str], root_dir: str
) -> None:
    hs3.dassert_is_valid_aws_profile(root_dir, aws_profile)
    if aws_profile == "ck":
        hdbg.dassert_in(
            root_dir,
            [
                FIXED_DATA_SNAPSHOTS_ROOT_DIR,
                DAILY_DATA_SNAPSHOT_ROOT_DIR,
                UNIT_TEST_DATA_DIR,
            ],
        )
