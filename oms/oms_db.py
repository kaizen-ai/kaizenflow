# Create a DB with this table

def get_create_target_files_table() -> str:
    """
    tradedate                                                     2021-11-12
    targetlistid                                                           1
    instanceid                                                          3504
    filename               s3://${bucket}/files/{foo}/{bar}/cand/targe...
    strategyid                                                       {strat}
    timestamp_processed                           2021-11-12 19:59:23.710677
    timestamp_db                                  2021-11-12 19:59:23.716732
    target_count                                                           1
    changed_count                                                          0
    unchanged_count                                                        0
    cancel_count                                                           0
    success                                                            False
    reason                 There were a total of 1 malformed requests in ...
    """


def wait_for_target_ack():
    """
    """
    # Iterate on a query until there is a row coming back.
    # Otherwise time out.
