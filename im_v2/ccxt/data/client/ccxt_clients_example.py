"""
Import as:

import im_v2.ccxt.data.client.ccxt_clients_example as imvcdcccex
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.db.db_utils as imvcddbut


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
    data_version: str,
    universe_version: str,
    dataset: str,
    contract_type: str,
    data_snapshot: str,
) -> imvcdccccl.CcxtHistoricalPqByTileClient:
    """
    Get `CcxtHistoricalPqByTileClient` object for the prod model reading CCXT
    historical or real-time data.

    :param data_version: version of stored on S3 data
        - "v2" is located on s3://../reorg
        - "v3" is located on s3://../v3
    """
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    if data_version == "v2":
        root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
        version = ""
        tag = ""
    elif data_version == "v3":
        root_dir = os.path.join(s3_bucket_path, "v3")
        version = "v1_0_0"
        tag = "downloaded_1min"
    else:
        raise ValueError(f"Invalid data version='{data_version}'.")
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
        version=version,
        tag=tag,
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


# #############################################################################
# CcxtSqlRealTimeImClient
# #############################################################################


def get_CcxtSqlRealTimeImClient_example1(
    universe_version: str, db_stage: str, table_name: str
) -> imvcdccccl.CcxtSqlRealTimeImClient:
    """
    Get a real-time DB client for CCXT data.

    :param db_stage: 'local', 'dev', 'prod'
    :param table_name: name of the DB table to connect to
    :return: CCXT real-time client
    """
    db_connection = imvcddbut.DbConnectionManager.get_connection(db_stage)
    client = imvcdccccl.CcxtSqlRealTimeImClient(
        universe_version, db_connection, table_name
    )
    return client


def get_CcxtSqlRealTimeImClient_example2(
    db_connection: hsql.DbConnection, resample_1min: bool
) -> imvcdccccl.CcxtSqlRealTimeImClient:
    """
    Get a real-time DB client for CCXT data.

    :param db_stage: 'local', 'dev', 'prod'
    :param resample_1min:
    :return: CCXT real-time client
    """
    universe_version = "infer_from_data"
    table_name = "ccxt_ohlcv_spot"
    client = imvcdccccl.CcxtSqlRealTimeImClient(
        universe_version, db_connection, table_name, resample_1min=resample_1min
    )
    return client
