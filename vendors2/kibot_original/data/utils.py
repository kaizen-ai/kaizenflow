"""
Import as:

import vendors.kibot.data.utils as kdut
"""

import os

import pandas as pd

import helpers.git as git


def get_kibot_exchange_mapping_path() -> str:
    """
    Return path to Kibot-exchange contract mapping.
    """
    root = git.get_client_root(False)
    mapping_path = os.path.join(root, "vendors/kibot/data/kibot_to_exchange.csv")
    return mapping_path


def read_kibot_exchange_mapping() -> pd.DataFrame:
    """
    Read Kibot-exchange contract mapping.
    """
    # Get path to the Kibot-to-CME mapping table.
    path_to_mapping = get_kibot_exchange_mapping_path()
    # Read the mapping table.
    kibot_to_cme_mapping = pd.read_csv(path_to_mapping, index_col="Kibot_symbol")
    return kibot_to_cme_mapping
