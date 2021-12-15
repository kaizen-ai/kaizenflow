"""
Import as:

import oms.mocked_portfolio as omocport
"""

# class MockedPortfolio(AbstractPortfolio):
class MockedPortfolio:
    """
    Implement an object that mocks a real OMS / portfolio backed by a DB where
    updates to the state representing portfolio holdings are asynchronous.

    The DB contains the following tables:
    - `positions`
        - current_position
        - open_quantity
    """

    def __init__(
        self,
        # TODO(gp): Add docstrings for these.
        strategy_id: str,
        account: str,
        #
        market_data_interface: cdtfprint.AbstractMarketDataInterface,
        asset_id_col: str,
        mark_to_market_col: str,
        timestamp_col: str,
        #
        db_connection: hsql.DbConnection,
        table_name: str,
        get_wall_clock: Any,
    ):
        """
        :param table_name: the name of the table containing the current positions
        :param asset_id_col: the name of the column storing the asset id
        """
        self._db_connection = db_connection
        self._table_name = table_name
        self._asset_id_col = asset_col_id
        # TODO(gp): Keep cash stored here.

    def _update_state(
        self,
        curr_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        asset_id = None
        new_holdings = self._get_current_holdings(curr_timestamp, asset_id)
        return new_holdings

    def _get_current_holdings(
        self,
        curr_timestamp: pd.Timestamp,
        asset_id: Optional[Any],
    ) -> pd.DataFrame:
        """
        Return the holdings at `timestamp`.
        """
        # Wait until the portfolio is stable.
        # TODO(gp): Implement.
        #
        # TODO(gp): How to compute the new cash amount?
        query = []
        query.append(f"SELECT id, current_position from {self._table_name}")
        trade_date = curr_timestamp.date()
        query.append(
            f"WHERE account={self._account} AND tradedate='{trade_date}'"
        )
        if asset_id is not None:
            query.append(f"AND {self._asset_id_col}={asset_id}")
        query.append(f"ORDER by {self._asset_id_col}")
        query = "\n".join(query)
        df = hsql.execute_query_to_df(self._db_connection, query)
        hdbg.dassert_lt(0, df.shape[0])
        # Convert a df like:
        # ```
        # ```
        # into a `holding_df`:
        # ```
        # ```
        return df
