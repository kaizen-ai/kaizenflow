"""
Import as:

import im_v2.ccxt.data.client.ccx_clients_example as icdccce
"""

import os

import helpers.dbg as hdbg
import helpers.git as hgit
import im_v2.ccxt.data.client.clients as imvcdclcl


# TODO(gp): @grisha, @dan how was this file generated?
def get_test_data_dir():
    test_data_dir = os.path.join(
        hgit.get_client_root(False),
        "im_v2/ccxt/data/client/test/test_data",
    )
    hdbg.dassert_dir_exists(test_data_dir)
    return test_data_dir


def get_CcxtCsvFileSytemClient_example1() -> imvcdclcl.CcxtCsvFileSystemClient:
    """
    Get `CcxtCsvFileSystemClient` object for the tests.
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
    data_type="ohlcv"
    root_dir=get_test_data_dir()
    ccxt_file_client = imvcdclcl.CcxtCsvFileSystemClient(
        data_type, root_dir
    )
    return ccxt_file_client


def get_CcxtParquetFileSytemClient_example1() -> imvcdclcl.CcxtParquetFileSystemClient:
    data_type = "ohlcv"
    root_dir=get_test_data_dir()
    ccxt_client = imvcdclcl.CcxtParquetFileSystemClient(data_type, root_dir)
    return ccxt_client
