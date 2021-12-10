"""
Implement the DB interface of a broker accepting orders.

Import as:

import oms.oms_db as oomsdb
"""

import logging
from typing import Any, Dict

import helpers.dbg as hdbg
import helpers.datetime_ as hdateti
import helpers.hasyncio as hasynci
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


# #############################################################################


SUBMITTED_ORDERS_TABLE_NAME = "submitted_orders"


def create_submitted_orders_table(
    db_connection: hsql.DbConnection,
    incremental: bool,
    *,
    table_name: str = SUBMITTED_ORDERS_TABLE_NAME,
) -> str:
    """
    Create a table storing the orders submitted to the system.
    """
    # - filename (e.g., s3://${bucket}/files/.../cand/targe...)
    #     = the filename we read. In this context, this is the full S3 key that
    #       you uploaded the file to.
    # - timestamp_db (e.g., 2021-11-12 19:59:23.716732)
    # - order_as_csv
    #     = target order in CSV format
    query = []
    if not incremental:
        query.append(f"DROP TABLE IF EXISTS {table_name}")
    query.append(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            filename VARCHAR(255) NOT NULL,
            timestamp_db TIMESTAMP NOT NULL,
            orders_as_txt VARCHAR(16384)
            )
            """
    )
    query = "; ".join(query)
    _LOG.debug("query=%s", query)
    db_connection.cursor().execute(query)
    return table_name


# #############################################################################


# This corresponds to the table "target_files_processed_candidate_view" of an
# implemented system.
ACCEPTED_ORDERS_TABLE_NAME = "accepted_orders"


def create_accepted_orders_table(
    db_connection: hsql.DbConnection,
    incremental: bool,
    *,
    table_name: str = ACCEPTED_ORDERS_TABLE_NAME,
) -> str:
    """
    Create a table for acknowledging that orders have been accepted.
    """
    # - strategyid (e.g., "strat")
    # - targetlistid (e.g., 1)
    #     = just an internal ID
    # - tradedate (e.g., 2021-11-12)
    # - instanceid (e.g., 3504)
    #     = refers to a number that determines a unique "run" of the continuous
    #       trading system service that polls S3 for targets and inserts them
    #       into the DB.
    #     - If we restarted the service intra-day, one would see an updated
    #       `instanceid`. This is just for internal book keeping.
    # - filename (e.g., s3://${bucket}/files/.../cand/targe...)
    #     = the filename we read. In this context, this is the full S3 key that
    #       you uploaded the file to.
    # - timestamp_processed (e..g, 2021-11-12 19:59:23.710677)
    # - timestamp_db (e.g., 2021-11-12 19:59:23.716732)
    # - target_count (e.g., 1)
    #     = number of targets in file
    # - changed_count (e.g., 0)
    #     = number of targets in the file which are different from the last
    #       requested target for the corresponding (account, symbol)
    #     - Targets are considered the "same" if the target position + algo +
    #       algo params are the same. If the target is the same, it is treated as a
    #       no-op and nothing is done, since we're already working to fill that
    #       target.
    #     - One can see zeroes for the changed/unchanged count fields is because
    #       the one target you're passing in is considered "malformed", so it's
    #       neither changed or nor unchanged.
    # - unchanged_count (e.g., 0)
    #     = number of targets in the file which are the same from the last
    #       requested target for the corresponding (account, symbol)
    # - cancel_count (e.g., 0)
    # - success (e.g., False)
    # - reason (e.g., There were a total of..)
    query = []
    if not incremental:
        query.append(f"DROP TABLE IF EXISTS {table_name}")
    query.append(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            targetlistid SERIAL PRIMARY KEY,
            tradedate DATE NOT NULL,
            instanceid INT,
            filename VARCHAR(255) NOT NULL,
            strategyid VARCHAR(64),
            timestamp_processed TIMESTAMP NOT NULL,
            timestamp_db TIMESTAMP NOT NULL,
            target_count INT,
            changed_count INT,
            unchanged_count INT,
            cancel_count INT,
            success BOOL,
            reason VARCHAR(255)
            )
            """
    )
    query = "; ".join(query)
    _LOG.debug("query=%s", query)
    db_connection.cursor().execute(query)
    return table_name


# #############################################################################


# This corresponds to the table "current_positions_candidate_view" of an
# implemented system.
CURRENT_POSITIONS_TABLE_NAME = "current_positions"


def create_current_positions_table(
    db_connection: hsql.DbConnection,
    incremental: bool,
    *,
    table_name: str = CURRENT_POSITIONS_TABLE_NAME,
) -> str:
    """
    Create a table holding the current positions.
    """
    query = []
    if not incremental:
        query.append(f"DROP TABLE IF EXISTS {table_name}")
    query.append(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            strategyid VARCHAR(64),
            account VARCHAR(64),
            tradedate DATE NOT NULL,
            id INT,
            timestamp_db TIMESTAMP NOT NULL,
            target_position INT,
            current_position INT,
            open_quantity INT,
            )
            """
    )
    query = "; ".join(query)
    _LOG.debug("query=%s", query)
    db_connection.cursor().execute(query)
    return table_name


# #############################################################################


async def wait_for_order_accepted(
    db_connection: hsql.DbConnection,
    target_value: str,
    poll_kwargs: Dict[str, Any],
    *,
    table_name: str = ACCEPTED_ORDERS_TABLE_NAME,
    field_name: str = "filename",
) -> hasynci.PollOutput:
    """
    Wait until the desired order is accepted by the system.

    The order is accepted when there is a value `target_value` in the column
    `file_name` of the expected table.

    :param poll_kwargs: a dictionary with the kwargs for `poll()`.
    """
    # Create a polling function that checks whether `target_value` is present
    # in the `field_name` column of the table `table_name`.
    polling_func = lambda: hsql.is_row_with_value_present(
        db_connection, table_name, field_name, target_value
    )
    # Poll.
    rc, result = await hasynci.poll(polling_func, **poll_kwargs)
    return rc, result


async def order_processor(
    db_connection: hsql.DbConnection,
    poll_kwargs: Dict[str, Any],
    get_wall_clock_time: hdateti.GetWallClockTime,
    delay_to_accept_in_secs: float,
    delay_to_fill_in_secs: float,
    portfolio,
    broker,
    *,
    submitted_orders_table_name: str = SUBMITTED_ORDERS_TABLE_NAME,
    accepted_orders_table_name: str = ACCEPTED_ORDERS_TABLE_NAME,
    current_positions_table_name: str = CURRENT_POSITIONS_TABLE_NAME,
) -> None:
    """
    A coroutine that:

    - polls for submitted orders
    - updates the accepted orders table
    - updates the current positions table

    :param delay_to_accept_in_secs: how long to wait after the order is submitted
        to update the accepted orders table
    """
    # Wait for orders to be written in `submitted_orders_table_name`.
    await hsql.wait_for_change_in_number_of_rows(
        db_connection, submitted_orders_table_name, poll_kwargs
    )
    # Get the new order.
    # TODO(gp): Implement.
    # Delay.
    hdbg.dassert_lt(0, delay_to_accept_in_secs)
    await hasynci.sleep(delay_to_accept_in_secs)
    # Write in `accepted_orders_table_name` to acknowledge the orders.
    timestamp_db = get_wall_clock_time()
    trade_date = timestamp_db.date()
    txt = f"""
    strategyid,SAU1
    account,candidate
    tradedate,{trade_date}
    id,1
    timestamp_db,{timestamp_db}
    target_position,
    current_position,
    open_quantity,
    """
    row = hsql.csv_to_series(txt, sep=",")
    hsql.execute_insert_query(db_connection, row, accepted_orders_table_name)
    # Wait.
    hdbg.dassert_lt(0, delay_to_fill_in_ses)
    await hasynci.sleep(delay_to_fill_in_secs)
    # Get the fills.
    fills = broker.get_fills()
    # Get the current holdings from the portfolio.
    holdings = None
    # Update the holdings with the fills.
    new_holdings = None
    # Write the new positions to `current_positions_table_name`.
