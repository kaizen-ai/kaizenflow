"""
Import as:

import im_v2.cryptodatadownload.data.client.cdd_clients_example as ivcdcccex
"""

import os

import helpers.hs3 as hs3
import im_v2.cryptodatadownload.data.client.cdd_client as imcdaclcd


def get_CcdClient_example1():
    data_type = "OHLCV"
    root_dir = os.path.join(hs3.get_path(), "data")
    cdd_client = imcdaclcd.CddClient(data_type, root_dir, aws_profile="am")
    return cdd_client