import asyncio
import io
import logging

import pandas as pd

import core.real_time as creatime
import helpers.hasyncio as hasynci
import helpers.hprint as hprint
import helpers.hsql as hsql
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.broker_example as obroexam
import oms.oms_db as oomsdb
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)

_5mins = pd.DateOffset(minutes=5)


# #############################################################################


class TestSimulatedPortfolio1(hunitest.TestCase):
    # @pytest.mark.skip("This is flaky because of the clock jitter")
    def test_state(self) -> None:
        """
        Check non-cash holdings for a Portfolio with only cash.
        """
        expected = r"""                           asset_id  curr_num_shares  price    value  \
2000-01-01 09:35:00-05:00        -1          1000000      1  1000000

                               wall_clock_timestamp
2000-01-01 09:35:00-05:00 2000-01-01 09:35:00-05:00  """
        portfolio = self._get_portfolio1()
        actual = portfolio.get_cached_mark_to_market()
        self.assert_equal(str(actual), expected, fuzzy_match=True)

    def _get_portfolio1(self):
        """
        Return a freshly minted Portfolio with only cash.
        """
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                _,
            ) = mdata.get_ReplayedTimeMarketData_example3(
                event_loop
            )
            # Build a Portfolio.
            portfolio = oporexam.get_simulated_portfolio_example1(
                event_loop,
                market_data=market_data,
            )
            return portfolio


# #############################################################################


class TestSimulatedPortfolio2(hunitest.TestCase):
    def test_initialization_with_cash1(self) -> None:
        """
        Initialize a Portfolio with cash.
        """
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                _,
            ) = mdata.get_ReplayedTimeMarketData_example3(
                event_loop
            )
            # Build Portfolio.
            portfolio = oporexam.get_simulated_portfolio_example1(
                event_loop,
                market_data=market_data,
            )
            # Check.
            expected = pd.DataFrame(
                {-1: 1000000.0},
                [
                    pd.Timestamp(
                        "2000-01-01 09:35:00-05:00", tz="America/New_York"
                    )
                ],
            )
            self.assert_dfs_close(portfolio.get_historical_holdings(), expected)

    def test_initialization_with_holdings1(self) -> None:
        """
        Initialize a Portfolio with holdings.
        """
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                get_wall_clock_time,
            ) = mdata.get_ReplayedTimeMarketData_example3(
                event_loop
            )
            # Build Broker.
            broker = obroexam.get_simulated_broker_example1(
                event_loop, market_data=market_data
            )
            # Build Portfolio.
            strategy_id = "str1"
            account = "paper"
            asset_id_col = "asset_id"
            mark_to_market_col = "price"
            timestamp_col = "end_datetime"
            holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
            portfolio = omportfo.SimulatedPortfolio.from_dict(
                strategy_id,
                account,
                broker,
                asset_id_col,
                mark_to_market_col,
                timestamp_col,
                holdings_dict=holdings_dict,
            )
            # Check.
            expected = pd.DataFrame(
                {101: 727.5, 202: 1040.3, -1: 10000.0},
                [
                    pd.Timestamp(
                        "2000-01-01 09:35:00-05:00", tz="America/New_York"
                    )
                ],
            )
            self.assert_dfs_close(portfolio.get_historical_holdings(), expected)

    def test_get_historical_statistics1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                _,
            ) = mdata.get_ReplayedTimeMarketData_example3(
                event_loop
            )
            #
            portfolio = oporexam.get_simulated_portfolio_example1(
                event_loop,
                market_data=market_data,
            )
            # Check.
            txt = r"""
,2000-01-01 09:35:00-05:00
net_asset_holdings,0
cash,1000000.0
net_wealth,1000000.0
gross_exposure,0.0
leverage,0.0
pnl,NaN
"""
            expected = pd.read_csv(
                io.StringIO(txt),
                index_col=0,
            )
            # The timestamp doesn't parse correctly from the CSV.
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:35:00-05:00", tz="America/New_York"
            )
            expected.columns = [initial_timestamp]
            actual = portfolio.get_historical_statistics().transpose()
            self.assert_dfs_close(actual, expected, rtol=1e-2, atol=1e-2)

    def test_historical_statistics2(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                get_wall_clock_time,
            ) = mdata.get_ReplayedTimeMarketData_example3(
                event_loop
            )
            # Build Broker.
            broker = obroexam.get_simulated_broker_example1(
                event_loop, market_data=market_data
            )
            # Build Portfolio.
            strategy_id = "str1"
            account = "paper"
            asset_id_col = "asset_id"
            mark_to_market_col = "price"
            timestamp_col = "end_datetime"
            holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
            portfolio = omportfo.SimulatedPortfolio.from_dict(
                strategy_id,
                account,
                broker,
                asset_id_col,
                mark_to_market_col,
                timestamp_col,
                holdings_dict=holdings_dict,
            )
            txt = r"""
,2000-01-01 09:35:00-05:00i
net_asset_holdings,1768351.42
cash,10000.0
net_wealth,1778351.42
gross_exposure,1768351.42
leverage,0.994
pnl,NaN
"""
            expected = pd.read_csv(
                io.StringIO(txt),
                index_col=0,
            )
            # The timestamp doesn't parse correctly from the CSV.
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:35:00-05:00", tz="America/New_York"
            )
            expected.columns = [initial_timestamp]
            actual = portfolio.get_historical_statistics().transpose()
            self.assert_dfs_close(actual, expected, rtol=1e-2, atol=1e-2)

    def test_get_historical_statistics3(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            tz = "ET"
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:35:00-05:00", tz="America/New_York"
            )
            get_wall_clock_time = creatime.get_replayed_wall_clock_time(
                tz,
                initial_timestamp,
                event_loop=event_loop,
            )
            price_txt = r"""
start_datetime,end_datetime,asset_id,price
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,100,100.34
"""
            price_df = pd.read_csv(
                io.StringIO(price_txt),
                parse_dates=["start_datetime", "end_datetime"],
            )
            start_time_col_name = "start_datetime"
            end_time_col_name = "end_datetime"
            knowledge_datetime_col_name = "end_datetime"
            delay_in_secs = 0
            asset_id_col_name = "asset_id"
            asset_ids = None
            columns = []
            market_data = mdata.ReplayedMarketData(
                price_df,
                knowledge_datetime_col_name,
                delay_in_secs,
                asset_id_col_name,
                asset_ids,
                start_time_col_name,
                end_time_col_name,
                columns,
                get_wall_clock_time,
            )
            portfolio = oporexam.get_simulated_portfolio_example1(
                event_loop,
                market_data=market_data,
            )
            # Check.
            txt = r"""
,2000-01-01 09:35:00-05:00
net_asset_holdings,0
cash,1000000.0
net_wealth,1000000.0
gross_exposure,0.0
leverage,0.0
pnl,NaN
"""
            expected = pd.read_csv(
                io.StringIO(txt),
                index_col=0,
            )
            # The timestamp doesn't parse correctly from the CSV.
            expected.columns = [initial_timestamp]
            actual = portfolio.get_historical_statistics().transpose()
            self.assert_dfs_close(actual, expected, rtol=1e-2, atol=1e-2)


# #############################################################################


def _get_row1() -> pd.Series:
    row = """
    strategyid,SAU1
    account,candidate
    id,0
    tradedate,2000-01-01
    timestamp_db,2000-01-01 21:38:39.419536
    asset_id,101
    target_position,10
    current_position,20.0
    open_quantity,0
    net_cost,0
    bod_position,0
    bod_price,0
    """
    srs = hsql.csv_to_series(row, sep=",")
    return srs


def _get_row2() -> pd.Series:
    row = """
    strategyid,SAU1
    account,candidate
    id,0
    tradedate,2000-01-01
    timestamp_db,2000-01-01 21:38:39.419536
    asset_id,101
    target_position,10
    current_position,20.0
    open_quantity,0
    net_cost,1903.1217
    bod_position,0
    bod_price,0
    """
    srs = hsql.csv_to_series(row, sep=",")
    return srs


class TestMockedPortfolio1(omtodh.TestOmsDbHelper):
    def test1(self) -> None:
        """
        Test that the update of Portfolio works.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create current positions in the table.
            row = _get_row1()
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            hsql.execute_insert_query(self.connection, row, table_name)
            if False:
                # Print the DB status.
                query = """SELECT * FROM current_positions"""
                df = hsql.execute_query_to_df(self.connection, query)
                print(hprint.dataframe_to_str(df))
                assert 0
            #
            # Create MockedPortfolio with some initial cash.
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                self.connection,
                table_name,
                asset_ids=[101],
            )
            coroutines = [self._coroutine1(portfolio)]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    def test2(self) -> None:
        """
        Test that the update of Portfolio works when accounting for costs.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create current positions in the table.
            row = _get_row2()
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            hsql.execute_insert_query(self.connection, row, table_name)
            if False:
                # Print the DB status.
                query = """SELECT * FROM current_positions"""
                df = hsql.execute_query_to_df(self.connection, query)
                print(hprint.dataframe_to_str(df))
                assert 0
            #
            # Create MockedPortfolio with some initial cash.
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                self.connection,
                table_name,
                asset_ids=[101],
            )
            coroutines = [self._coroutine2(portfolio)]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _coroutine1(
        self,
        portfolio,
    ):
        await asyncio.sleep(60 * 5)
        portfolio.mark_to_market()
        # Check.
        actual = str(portfolio)
        expected = r"""# historical holdings=
asset_id                    101       -1
2000-01-01 09:35:00-05:00   0.0  1000000.0
2000-01-01 09:40:00-05:00  20.0  1000000.0
# historical holdings marked to market=
asset_id                            101       -1
2000-01-01 09:35:00-05:00      0.000000  1000000.0
2000-01-01 09:40:00-05:00  20004.027347  1000000.0
# historical statistics=
                           net_asset_holdings       cash    net_wealth  gross_exposure  leverage           pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.0  1.000000e+06        0.000000  0.000000           NaN
2000-01-01 09:40:00-05:00        20004.027347  1000000.0  1.020004e+06    20004.027347  0.019612  20004.027347"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    async def _coroutine2(
        self,
        portfolio,
    ):
        await asyncio.sleep(60 * 5)
        portfolio.mark_to_market()
        # Check.
        actual = str(portfolio)
        expected = r"""# historical holdings=
asset_id                    101          -1
2000-01-01 09:35:00-05:00   0.0  1000000.0000
2000-01-01 09:40:00-05:00  20.0   998096.8783
# historical holdings marked to market=
asset_id                            101          -1
2000-01-01 09:35:00-05:00      0.000000  1000000.0000
2000-01-01 09:40:00-05:00  20004.027347   998096.8783
# historical statistics=
                           net_asset_holdings          cash    net_wealth  gross_exposure  leverage           pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.0000  1.000000e+06        0.000000  0.000000           NaN
2000-01-01 09:40:00-05:00        20004.027347   998096.8783  1.018101e+06    20004.027347  0.019648  18100.905647"""
        self.assert_equal(actual, expected, fuzzy_match=True)
