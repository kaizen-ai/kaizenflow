"""
Import as:

import im_v2.talos.data.client.talos_clients_example as imvtdctcex
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hs3 as hs3
import im_v2.talos.data.client.talos_clients as imvtdctacl


# TODO(gp): We should have unit tests to document how this data was generated.
def get_test_data_dir() -> str:
    """
    Get dir with data files for the tests.

    The files in the dir are copies of some `Talos` data files from S3
    that were loaded for our research purposes. These copies are checked
    out locally in order to test functions without dependencies on S3.

    Test snippets contain altered original data (3rd and 4th minutes are removed).
    This is done in order to demonstrate that `resample_1min` works correctly.
    """
    test_data_dir = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/talos/data/client/test/test_data",
    )
    hdbg.dassert_dir_exists(test_data_dir)
    return test_data_dir


def get_TalosHistoricalPqByTileClient_example1(
    resample_1min: bool,
) -> imvtdctacl.TalosHistoricalPqByTileClient:
    """
    Get `TalosHistoricalPqByTileClient` object for the tests.
    """
    universe_version = "small"
    root_dir = get_test_data_dir()
    partition_mode = "by_year_month"
    data_snapshot = "latest"
    talos_file_client = imvtdctacl.TalosHistoricalPqByTileClient(
        universe_version,
        resample_1min,
        root_dir,
        partition_mode,
        data_snapshot,
    )
    return talos_file_client


def get_TalosHistoricalPqByTileClient_example2(
    resample_1min: bool,
) -> imvtdctacl.TalosHistoricalPqByTileClient:
    """
    Get `TalosHistoricalPqByTileClient` object for the tests from S3.
    """
    universe_version = "small"
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
    partition_mode = "by_year_month"
    data_snapshot = "latest"
    talos_parquet_client = imvtdctacl.TalosHistoricalPqByTileClient(
        universe_version,
        resample_1min,
        root_dir,
        partition_mode,
        data_snapshot,
        aws_profile="ck",
    )
    return talos_parquet_client
