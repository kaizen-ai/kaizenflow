import logging

import pandas as pd

_LOG = logging.getLogger(__name__)

_PRODUCT_SPECS_PATH = "/data/prices/product_slate_export_with_contract_specs_20190905.csv"
_PRODUCT_LIST_PATH = "/data/prices/product_slate_export_20190904.csv"


def read_product_specs(file_path=_PRODUCT_SPECS_PATH):
    """
    Read a csv file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    _LOG.info("Reading %s", file_path)
    df = pd.read_csv(file_path)
    return df


def read_product_list(file_path=_PRODUCT_LIST_PATH):
    """
    Read a csv file into pandas.

    :param file_path: path to the file
    :return: pd.DataFrame
    """
    _LOG.info("Reading %s", file_path)
    df = pd.read_csv(file_path)
    return df
