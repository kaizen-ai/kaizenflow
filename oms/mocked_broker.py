"""
Import as:

import oms.mocked_broker as omocbrok
"""


class MockedBroker:
    """
    Implement an object that mocks a real OMS / broker backed by a DB where
    updates to the state representing the placed orders are asynchronous.

    The DB contains the following tables:
    - `processed`
        - tradedate
        - strategyid
        - timestamp_db
            - when the order list was received from the OMS

    - A more complex implementation can also have
        - target_count
        - changed_count
        - unchanged_count
    """

    def __init__(self):
        # TODO(gp): Pass the DB connection info.
        pass

    def submit_orders(
        self,
        orders: List[omorder.Order],
    ):
        # TODO(gp): Write into `OmsDb.submitted_orders`.
        # TODO(gp): Wait on `OmsDb.processed_orders`.
        pass

    def get_fills(self, curr_timestamp: pd.Timestamp) -> List[Fill]:
        """
        The reference system doesn't return fills but directly updates the
        state of a table representing the current holdings.
        """
        raise NotImplemented
