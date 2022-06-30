"""
Import as:

import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce
"""

import os

import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as imvccdcccc


def get_CryptoChassisHistoricalPqByTileClient_example1(
    resample_1min: bool,
) -> imvccdcccc.CryptoChassisHistoricalPqByTileClient:
    """
    Get `CryptoChassisHistoricalPqByTileClient` object for the prod model
    reading actual historical data, which is stored on S3.

    Client is initialized to process CryptoChassis data for:
    - universe version: "v1"
    - contract type: "spot"
    - data snapshot: "latest"
    """
    universe_version = "v1"
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
    partition_mode = "by_year_month"
    dataset = "ohlcv"
    contract_type = "spot"
    crypto_chassis_parquet_client = (
        imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            aws_profile=aws_profile,
        )
    )
    return crypto_chassis_parquet_client


def get_CryptoChassisHistoricalPqByTileClient_example2(
    resample_1min: bool,
) -> imvccdcccc.CryptoChassisHistoricalPqByTileClient:
    """
    Get `CryptoChassisHistoricalPqByTileClient` object for the prod model
    reading actual historical data, which is stored on S3.

    Client is initialized to process CryptoChassis data for:
    - universe version: "v2"
    - contract type: "spot"
    - data snapshot: "20220530"
    """
    universe_version = "v2"
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
    partition_mode = "by_year_month"
    dataset = "ohlcv"
    contract_type = "spot"
    data_snapshot = "20220530"
    crypto_chassis_parquet_client = (
        imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=aws_profile,
        )
    )
    return crypto_chassis_parquet_client


def get_CryptoChassisHistoricalPqByTileClient_example3(
    resample_1min: bool,
) -> imvccdcccc.CryptoChassisHistoricalPqByTileClient:
    """
    Get `CryptoChassisHistoricalPqByTileClient` object for the prod model
    reading actual futures historical data, which is stored on S3.

    Client is initialized to process CryptoChassis data for:
    - universe version: "v3"
    - contract type: "futures"
    - data snapshot: "20220620"
    """
    universe_version = "v3"
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
    partition_mode = "by_year_month"
    dataset = "ohlcv"
    contract_type = "futures"
    data_snapshot = "20220620"
    crypto_chassis_parquet_client = (
        imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=aws_profile,
        )
    )
    return crypto_chassis_parquet_client
