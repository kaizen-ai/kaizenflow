"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl

# TODO(gp): @Grisha -> move the example file in parallel to ccxt_clients
#  We want to expose this from the package, we don't like exporting stuff from
#  test.


def get_test_data_dir() -> str:
    """
    Get dir with data files for the tests.

    The files in the dir are copies of some `CCXT` data files from S3
    that were loaded for our research purposes. These copies are checked
    out locally in order to test functions without dependencies on S3.
    """
    test_data_dir = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/ccxt/data/client/test/test_data",
    )
    hdbg.dassert_dir_exists(test_data_dir)
    return test_data_dir


def get_CcxtCsvClient_example1(
    resample_1min: bool,
) -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv.gz`.
    """
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "csv.gz"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, resample_1min, root_dir, extension
    )
    return ccxt_file_client


def get_CcxtCsvClient_example2() -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv`.
    """
    resample_1min = True
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "csv"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, resample_1min, root_dir, extension
    )
    return ccxt_file_client


def get_CcxtParquetByAssetClient_example1(
    resample_1min: bool,
) -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `pq`.
    """
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "pq"
    ccxt_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, resample_1min, root_dir, extension
    )
    return ccxt_client
