"""
Import as:

import oms.restrictions_example as oresexam
"""

import logging

import pandas as pd

import helpers.hsql as hsql
import oms.oms_db as oomsdb

_LOG = logging.getLogger(__name__)

import oms.restrictions as omrestri


def get_Restrictions_example1(
    db_connection: hsql.DbConnection,
    *,
    table_name: str = oomsdb.RESTRICTIONS_TABLE_NAME,
) -> omrestri.Restrictions:
    strategy_id = "st1"
    account = "paper"
    asset_id_col = "asset_id"
    date_col = "tradedate"
    get_wall_clock_time = lambda: pd.Timestamp("2000-01-01 22:00:00.12345")
    restrictions = omrestri.Restrictions(
        strategy_id,
        account,
        asset_id_col,
        date_col,
        db_connection,
        table_name,
        get_wall_clock_time,
    )
    return restrictions