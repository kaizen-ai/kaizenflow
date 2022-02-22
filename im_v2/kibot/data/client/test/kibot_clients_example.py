"""
Import as:

import im_v2.kibot.data.client.test.kibot_clients_example as ikidctkce
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.kibot.data.client.kibot_clients as imvkdckicl


def get_test_data_dir() -> str:
    """
    Get dir with data files for the tests.

    The files in the dir are copies of some Kibot data files from S3
    that were loaded for our research purposes. These copies are checked
    out locally in order to test functions without dependencies on S3.
    """
    test_data_dir = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/kibot/data/client/test/test_data",
    )
    hdbg.dassert_dir_exists(test_data_dir)
    return test_data_dir


def get_KibotEquitiesCsvParquetByAssetClient_example1(unadjusted: bool) -> imvkdckicl.KibotEquitiesCsvParquetByAssetClient:
    """
    Return a Kibot object with:
       - local (non-S3) `csv.gz` data
       - using equity data

    :param unadjusted: whether asset class prices are unadjusted (i.e., True or False)
    """
    # Get path to the dir with the test data.
    #
    # The data looks like:
    # ```
    # ,09/29/2015,08:24,102.99,102.99.1,102.99.2,102.99.3,112
    # 0,09/29/2015,08:27,102.99,102.99,102.99,102.99,112
    # 1,09/29/2015,09:04,103.18,103.18,103.18,103.18,781
    # 2,09/29/2015,09:15,102.63,102.63,102.63,102.63,112
    # 3,09/29/2015,09:17,102.56,102.56,102.56,102.56,112
    # ```
    # Initialize client.
    root_dir = get_test_data_dir()
    extension = "csv.gz"
    asset_class = "stocks"
    kibot_file_client = imvkdckicl.KibotEquitiesCsvParquetByAssetClient(
        root_dir,
        extension,
        asset_class,
        unadjusted,
    )
    return kibot_file_client


def get_KibotEquitiesCsvParquetByAssetClient_example2(unadjusted: bool) -> imvkdckicl.KibotEquitiesCsvParquetByAssetClient:
    """
    Return a Kibot object with:
       - local (non-S3) `pq` data
       - using equity data

    :param unadjusted: whether asset class prices are unadjusted (i.e., True or False)
    """
    root_dir = get_test_data_dir()
    extension = "pq"
    asset_class = "stocks"
    kibot_file_client = imvkdckicl.KibotEquitiesCsvParquetByAssetClient(
        root_dir,
        extension,
        asset_class,
        unadjusted,
    )
    return kibot_file_client


def get_KibotFuturesCsvParquetByAssetClient_example1(contract_type: str) -> imvkdckicl.KibotFuturesCsvParquetByAssetClient:
    """
    Return a Kibot object with:
       - local (non-S3) `csv.gz` data
       - using futures data

    :param contract_type: futures contract type (e.g., "continuous", "expiry")
    """
    root_dir = get_test_data_dir()
    extension = "csv.gz"
    kibot_file_client = imvkdckicl.KibotFuturesCsvParquetByAssetClient(
        root_dir,
        extension,
        contract_type,
    )
    return kibot_file_client


def get_KibotFuturesCsvParquetByAssetClient_example2(contract_type: str) -> imvkdckicl.KibotFuturesCsvParquetByAssetClient:
    """
    Return a Kibot object with:
       - local (non-S3) `pq` data
       - using futures data

    :param contract_type: futures contract type (e.g., "continuous", "expiry")
    """
    root_dir = get_test_data_dir()
    extension = "pq"
    kibot_file_client = imvkdckicl.KibotFuturesCsvParquetByAssetClient(
        root_dir,
        extension,
        contract_type,
    )
    return kibot_file_client



