# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %% [markdown]
# # Description

# %% [markdown]
# This notebook is used to regenerate the Kibot-exchange mapping table from source google spreadsheet.

import logging

import pandas as pd

import gspread_pandas
# %%
# %load_ext autoreload
# %autoreload 2
import helpers.dbg as dbg
import helpers.env as env
import helpers.printing as prnt

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", env.get_system_signature()[0])

prnt.config_notebook()


# %%
def generate_kibot_to_exchange_mapping() -> pd.DataFrame:
    """
    Load the Kibot-to-exchange mapping table and save as .csv file.

    The mapping is based on the following spreadsheet:
    https://docs.google.com/spreadsheets/d/1X1brCCTDtkKCegnX9fn-TN8PZXGhuPyrdMAsznLgzx4/edit#gid=1571752038  # pylint: disable=line-too-long

    The following columns are present:
         - "Exchange_group" for high-level exchanges' group
         - "Exchange_abbreviation" for exchange abbreviation
         - "Exchange_symbol" for contract designation in given exchange

    :return: Kibot-exchange mapping table.
    """
    # Load the spreadsheet with manual mapping.
    spreadsheet = gspread_pandas.spread.Spread(
        "PartTask1788 - Map Kibot to CME", sheet="exchange_mapping_csv"
    )
    mapping_df = spreadsheet.sheet_to_df()
    # Use default exchange symbol from CME/Kibot metadata or its manual correction.
    mapping_df.loc[
        mapping_df["Exchange_symbol"] == "", "Exchange_symbol"
    ] = mapping_df["Globex"]
    # Use default exchange abbreviation from CME/Kibot metadata or its correction.
    mapping_df.loc[
        mapping_df["Exchange_abbreviation"] == "", "Exchange_abbreviation"
    ] = mapping_df["Exchange_CME"]
    # Assign exchange group.
    mapping_df["Exchange_group"] = mapping_df["Exchange_abbreviation"].apply(
        _get_kibot_exchange_group
    )
    # Subset only correct mapping columns.
    mapping_df = mapping_df[
        ["Exchange_group", "Exchange_abbreviation", "Exchange_symbol"]
    ]
    # Save the mapping table.
    mapping_df.to_csv(_get_kibot_exchange_mapping_path())
    return mapping_df


# %%
if False:
    generate_kibot_to_exchange_mapping()
