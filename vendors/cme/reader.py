import logging
import os

import pandas as pd

import helpers.s3 as hs3

_LOG = logging.getLogger(__name__)


def read_product_specs(file_path=None):
    """
    Read a csv file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    if file_path is None:
        file_path = os.path.join(
            hs3.get_path(),
            "cme/product_slate_export_with_contract_specs_20190905.csv",
        )
    _LOG.info("Reading %s", file_path)
    df = pd.read_csv(file_path)
    return df


def read_product_list(file_path=None):
    """
    Read a csv file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    if file_path is None:
        file_path = os.path.join(
            hs3.get_path(), "cme/product_slate_export_20190904.csv"
        )
    _LOG.info("Reading %s", file_path)
    df = pd.read_csv(file_path)
    return df
