"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hs3 as hs3
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


# #############################################################################
# CcxtCsvClient
# #############################################################################


def get_CcxtCsvClient_example1(
    resample_1min: bool,
) -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv.gz`.
    """
    vendor = "CCXT"
    universe_version = "small"
    root_dir = get_test_data_dir()
    extension = "csv.gz"
    data_snapshot = "latest"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor,
        universe_version,
        root_dir,
        extension,
        data_snapshot,
        resample_1min=resample_1min,
    )
    return ccxt_file_client


def get_CcxtCsvClient_example2() -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `csv`.
    """
    resample_1min = True
    vendor = "CCXT"
    universe_version = "small"
    root_dir = get_test_data_dir()
    extension = "csv"
    data_snapshot = "latest"
    ccxt_file_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor,
        universe_version,
        root_dir,
        extension,
        data_snapshot,
        resample_1min=resample_1min,
    )
    return ccxt_file_client


# #############################################################################
# CcxtParquetByAssetClient
# #############################################################################


def get_CcxtParquetByAssetClient_example1(
    resample_1min: bool,
) -> imvcdccccl.CcxtCddCsvParquetByAssetClient:
    """
    Get `CcxtCddCsvParquetByAssetClient` object for the tests.

    Extension is `pq`.
    """
    vendor = "CCXT"
    universe_version = "small"
    root_dir = get_test_data_dir()
    extension = "pq"
    data_snapshot = "latest"
    ccxt_client = imvcdccccl.CcxtCddCsvParquetByAssetClient(
        vendor,
        universe_version,
        root_dir,
        extension,
        data_snapshot,
        resample_1min=resample_1min,
    )
    return ccxt_client


# #############################################################################
# CcxtHistoricalPqByTileClient
# #############################################################################


def get_CcxtHistoricalPqByTileClient_example1(
    # TODO(Grisha): make it optional since it is not needed for real-time data.
    universe_version: str,
    dataset: str,
    contract_type: str,
    data_snapshot: str,
) -> imvcdccccl.CcxtHistoricalPqByTileClient:
    """
    Get `CcxtHistoricalPqByTileClient` object for the prod model reading CCXT
    historical or real-time data.
    """
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    if data_snapshot == "":
        # Use the data updated in real-time.
        root_dir = os.path.join(
            s3_bucket_path, "reorg", "daily_staged.airflow.pq"
        )
    else:
        # Use the the historical data.
        root_dir = os.path.join(
            s3_bucket_path, "reorg", "historical.manual.pq"
        )
    resample_1min = False
    partition_mode = "by_year_month"
    ccxt_parquet_client = imvcdccccl.CcxtHistoricalPqByTileClient(
        universe_version,
        root_dir,
        partition_mode,
        dataset,
        contract_type,
        data_snapshot,
        aws_profile=aws_profile,
        resample_1min=resample_1min,
    )
    return ccxt_parquet_client


def get_CcxtHistoricalPqByTileClient_example2(
    resample_1min: bool,
) -> imvcdccccl.CcxtHistoricalPqByTileClient:
    """
    Get `CcxtHistoricalPqByTileClient` object for the tests reading data
    snippets created for unit tests.

    Client is initialized to process CCXT data for:
    - universe version: "small"
    - contract type: "spot"
    """
    # TODO(gp): express this guy in terms of get_CcxtHistoricalPqByTileClient_example1
    #  but the problem is that this uses "unit_test" instead of "reorg".
    universe_version = "small"
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "unit_test", "historical.manual.pq")
    partition_mode = "by_year_month"
    dataset = "ohlcv"
    contract_type = "spot"
    data_snapshot = "20220705"
    ccxt_parquet_client = imvcdccccl.CcxtHistoricalPqByTileClient(
        universe_version,
        root_dir,
        partition_mode,
        dataset,
        contract_type,
        data_snapshot,
        aws_profile=aws_profile,
        resample_1min=resample_1min,
    )
    return ccxt_parquet_client
