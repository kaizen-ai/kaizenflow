"""
Import as:

import oms.oms_db as oomsdb
"""

import asyncio
import logging
import math
from typing import Any, Callable, Tuple

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.printing as hprint
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)

# TODO(gp): Instead of returning the query just perform it. We should return the
#  query only when we want to freeze the query in a test.
def get_create_target_files_table_query(incremental: bool) -> str:
    """
    Create a table for `target_files`

    :param incremental: if it already exists and `incremental` is:
        - True: delete and create it from scratch
        - False: skip creating it
    """
    # targetlistid                                              1
    #   = just an internal ID.
    # tradedate                                        2021-11-12
    # instanceid                                             3504
    #   = refers to a number that determines a unique "run" of the continuous
    #     trading system service that polls S3 for targets and inserts them
    #     into the DB.
    #   - If we restarted the service intra-day, one would see an updated
    #     `instanceid`. This is just for internal book keeping.
    # filename             s3://${bucket}/files/.../cand/targe...
    #   = the filename we read. In this context, this is the full S3 key that
    #     you uploaded the file to.
    # strategyid                                          {strat}
    # timestamp_processed              2021-11-12 19:59:23.710677
    # timestamp_db                     2021-11-12 19:59:23.716732
    # target_count                                              1
    #   = number of targets in file
    # changed_count                                             0
    #   = number of targets in the file which are different from the last
    #     requested target for the corresponding (account, symbol)
    #   - Targets are considered the "same" if the target position + algo +
    #     algo params are the same. If the target is the same, it is treated as a
    #     no-op and nothing is done, since we're already working to fill that
    #     target.
    #   - One can see zeroes for the changed/unchanged count fields is because
    #     the one target you're passing in is considered "malformed", so it's
    #     neither changed or nor unchanged.
    # unchanged_count                                           0
    #   = number of targets in the file which are the same from the last
    #     requested target for the corresponding (account, symbol)
    # cancel_count                                              0
    # success                                               False
    # reason                              There were a total of..
    table_name = "target_files_processed_candidate_view"
    query = []
    if incremental:
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
    return query


async def poll(
    func: Callable,
    sleep_in_secs: float,
    timeout_in_secs: float,
    get_wall_clock_time: hdateti.GetWallClockTime,
) -> Tuple[int, Any]:
    """
    :param func: function returning a tuple (rc, value) where rc == 0 means keep
        iterating
    """
    _LOG.debug(hprint.to_str("func sleep_in_secs timeout_in_secs"))
    hdbg.dassert_lt(0, sleep_in_secs)
    hdbg.dassert_lt(0, timeout_in_secs)
    max_num_iter = math.ceil(timeout_in_secs / sleep_in_secs)
    hdbg.dassert_lte(1, max_num_iter)
    num_iter = 1
    while True:
        _LOG.debug(
            "Iter %s/%s: wall clock time=%s",
            num_iter + 1,
            max_num_iter,
            get_wall_clock_time(),
        )
        rc, value = func()
        _LOG.debug("rc=%s, value=%s", rc, value)
        if rc != 0:
            # The function returned.
            _LOG.debug(
                "poll done: wall clock time=",
                num_iter + 1,
                max_num_iter,
                get_wall_clock_time(),
            )
            return rc, value
        #
        num_iter += 1
        if num_iter >= max_num_iter:
            raise RuntimeError(
                "Timeout for "
                + hprint.to_str("func sleep_in_secs timeout_in_secs")
            )
        await asyncio.sleep(sleep_in_secs)


def wait_for_row(
    connection: hsql.DbConnection, target_value: str
) -> Tuple[int, pd.DataFrame]:
    table_name = "target_files_processed_candidate_view"
    query = f"SELECT filename FROM {table_name} WHERE filename='{target_value}'"
    df = hsql.execute_query(connection, query)
    _LOG.debug("df=\n%s", hprint.dataframe_to_str(df))
    rc = df.shape[0] > 0
    return rc, df


async def wait_for_target_ack(
    connection: hsql.DbConnection, target_value: str, poll_kwargs
) -> Tuple[int, pd.DataFrame]:
    """
    
    """
    func = lambda: wait_for_row(connection, target_value)
    rc, df = await poll(func, **poll_kwargs)
    return rc, df
