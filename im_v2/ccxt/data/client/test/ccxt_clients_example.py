"""
Import as:

import im_v2.ccxt.data.client.test.ccxt_clients_example as ivcdctcce
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl


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


def get_CcxtCsvClient_example1() -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv.gz`.
    """
    # Get path to the dir with the test data.
    #
    # The data looks like:
    # ```
    # timestamp,open,high,low,close,volume
    # 1534464060000,286.712987,286.712987,286.712987,286.712987,0.0175
    # 1534464120000,286.405988,286.405988,285.400193,285.400197,0.1622551
    # 1534464180000,285.400193,285.400193,285.400193,285.400193,0.0202596
    # 1534464240000,285.400193,285.884638,285.400193,285.884638,0.074655
    # ```
    # Initialize client.
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "csv.gz"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, root_dir, extension
    )
    return ccxt_file_client


def get_CcxtCsvClient_example2() -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv`.
    """
    # Get path to the dir with the test data.
    #
    # The data looks like:
    # ```
    # timestamp,open,high,low,close,volume
    # 1534464060000,286.712987,286.712987,286.712987,286.712987,0.0175
    # 1534464120000,286.405988,286.405988,285.400193,285.400197,0.1622551
    # 1534464180000,285.400193,285.400193,285.400193,285.400193,0.0202596
    # 1534464240000,285.400193,285.884638,285.400193,285.884638,0.074655
    # ```
    # Initialize client.
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "csv"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, root_dir, extension
    )
    return ccxt_file_client


def get_CcxtParquetByAssetClient_example1() -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `pq`.
    """
    # Get path to the dir with the test data.
    #
    # The data looks like:
    # ```
    # timestamp,open,high,low,close,volume
    # 1534464060000,286.712987,286.712987,286.712987,286.712987,0.0175
    # 1534464120000,286.405988,286.405988,285.400193,285.400197,0.1622551
    # 1534464180000,285.400193,285.400193,285.400193,285.400193,0.0202596
    # 1534464240000,285.400193,285.884638,285.400193,285.884638,0.074655
    # ```
    # Initialize client.
    vendor = "CCXT"
    root_dir = get_test_data_dir()
    extension = "pq"
    ccxt_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor, root_dir, extension
    )
    return ccxt_client
