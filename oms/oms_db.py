"""
Implement the DB interface of a broker accepting orders.

Import as:

import oms.oms_db as oomsdb
"""

import logging
from typing import Any, Dict

import helpers.hasyncio as hasynci
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


SUBMITTED_ORDERS_TABLE_NAME = "submitted_orders"


def create_submitted_orders_table(
    connection: hsql.DbConnection,
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
            order_as_csv VARCHAR(4096)
            )
            """
    )
    query = "; ".join(query)
    _LOG.debug("query=%s", query)
    connection.cursor().execute(query)
    return table_name


# #############################################################################


# This corresponds to the table "target_files_processed_candidate_view" of an
# implemented system.
ACCEPTED_ORDERS_TABLE_NAME = "accepted_orders"


def create_accepted_orders_table(
    connection: hsql.DbConnection,
    incremental: bool,
    *,
    table_name: str = ACCEPTED_ORDERS_TABLE_NAME,
) -> str:
    """
    Create a table for acknowledging that orders have been accepted.
    """
    # - targetlistid (e.g., 1)
    #    = just an internal ID
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
    # - strategyid (e.g., "strat")
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
    connection.cursor().execute(query)
    return table_name


# #############################################################################


async def wait_for_order_accepted(
    connection: hsql.DbConnection,
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
        connection, table_name, field_name, target_value
    )
    # Poll.
    rc, result = await hasynci.poll(polling_func, **poll_kwargs)
    return rc, result


# #############################################################################


async def order_processor(
    connection: hsql.DbConnection,
    delay_in_secs: float,
    poll_kwargs: Dict[str, Any],
    *,
    src_table_name: str = SUBMITTED_ORDERS_TABLE_NAME,
    dst_table_name: str = ACCEPTED_ORDERS_TABLE_NAME,
) -> None:
    """
    A coroutine that polls for submitted orders and update the accepted orders
    table.

    :param delay_in_secs: how long to wait after the order is submitted to update
        the accepted orders table
    """
    # Wait for orders to be written in `src_table_name`.
    await hsql.wait_for_change_in_number_of_rows(
        connection, src_table_name, poll_kwargs
    )
    # Wait.
    await hasynci.wait(delay_in_secs)
    # Write in `dst_table_name` to acknowledge the orders.
