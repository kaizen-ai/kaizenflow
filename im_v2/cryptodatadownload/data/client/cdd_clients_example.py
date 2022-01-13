"""
Import as:

import im_v2.cryptodatadownload.data.client.cdd_clients_example as imvcrcdcex
"""

import os

import helpers.hs3 as hs3
import im_v2.cryptodatadownload.data.client.cdd_client as imcdaclcd


def get_CcdClient_example1():
    data_type = "OHLCV"
    # Note for reviewer: root_dir was previously defined as "_AM_S3_ROOT_DIR"
    # but the test keep failing on that, so I untroduced a solution
    # that fixes it. Is it ok to use this?
    root_dir = os.path.join(hs3.get_path(), "data") 
    cdd_client = imcdaclcd.CddClient(data_type, root_dir, aws_profile="am")
    return cdd_client
