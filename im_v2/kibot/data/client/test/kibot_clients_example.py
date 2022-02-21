"""
Import as:

import im_v2.kibot.data.client.test.kibot_clients_example as ikidctkce
"""

import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.common.data.client.test.im_client_test_case as icdctictc
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



