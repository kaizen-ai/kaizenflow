"""
Import as:

import im_v2.talos.data.client.talos_clients_example as imvtdctcex
"""

import os

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.talos.data.client.talos_clients as imvtdctacl


def get_test_data_dir() -> str:
    """
    Get dir with data files for the tests.

    The files in the dir are copies of some `Talos` data files from S3
    that were loaded for our research purposes. These copies are checked
    out locally in order to test functions without dependencies on S3.
    """
    test_data_dir = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/talos/data/client/test/test_data",
    )
    hdbg.dassert_dir_exists(test_data_dir)
    return test_data_dir


def get_TalosParquetByTileClient_example1() -> imvtdctacl.TalosParquetByTileClient:
    """
    Get `TalosParquetByTileClient` object for the tests.
    """
    root_dir = get_test_data_dir()
    talos_file_client = imvtdctacl.TalosParquetByTileClient(root_dir)
    return talos_file_client