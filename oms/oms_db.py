"""
Import as:

import oms.oms_db as oomsdb
"""

# Create a DB with this table


def get_create_target_files_table_query() -> str:
    """
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
    query = """
    CREATE TABLE IF NOT EXISTS target_files_processed_candidate_view(
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
    return query


def wait_for_target_ack(connection, query, sleep_in_secs: float,
                        timeout_in_secs):
    """
    
    """
    query = ""

    # Iterate on a query until there is a row coming back.
    # Otherwise time out.
