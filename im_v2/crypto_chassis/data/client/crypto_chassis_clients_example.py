"""
Import as:

import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce
"""

import os

import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as imvccdcccc


def get_CryptoChassisHistoricalPqByTileClient_example1(
    universe_version: str,
    resample_1min: bool,
    dataset: str,
    contract_type: str,
    data_snapshot: str,
) -> imvccdcccc.CryptoChassisHistoricalPqByTileClient:
    """
    Get `CryptoChassisHistoricalPqByTileClient` object for the prod model
    reading actual historical data, which is stored on S3.
    """
    aws_profile = "ck"
    s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
    root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
    partition_mode = "by_year_month"
    crypto_chassis_parquet_client = (
        imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            data_snapshot,
            aws_profile=aws_profile,
        )
    )
    return crypto_chassis_parquet_client