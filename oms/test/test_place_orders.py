import io
import logging

import numpy as np
import pandas as pd

import core.config as cconfig
import core.dataflow.test.test_price_interface as dartttdi
import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import oms.place_orders as oplaorde
import oms.portfolio as omportfo
import oms.test.test_portfolio as ottport

_LOG = logging.getLogger(__name__)

import oms.test.test_portfolio as ottport


class TestPlaceOrders1(hunitest.TestCase):
    def test1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            config = {}
            # # Build a ReplayedTimePriceInterface.
            # start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
            # end_datetime = pd.Timestamp("2000-01-01 10:30:00-05:00")
            # initial_replayed_delay = 5
            # delay_in_secs = 0
            # asset_ids = [101, 202]
            # columns = ["midpoint"]
            # sleep_in_secs = 30
            # time_out_in_secs = 60 * 5
            # rtpi = dartttdi.get_replayed_time_price_interface_example1(
            #     event_loop,
            #     start_datetime,
            #     end_datetime,
            #     initial_replayed_delay,
            #     delay_in_secs,
            #     asset_ids=asset_ids,
            #     columns=columns,
            #     sleep_in_secs=sleep_in_secs,
            #     time_out_in_secs=time_out_in_secs,
            # )
            rtpi = ottport.get_replayed_time_price_interface(event_loop)
            # Build predictions.
            index = [
                pd.Timestamp("2000-01-01 09:35:00-05:00"),
                pd.Timestamp("2000-01-01 09:40:00-05:00"),
                pd.Timestamp("2000-01-01 09:45:00-05:00"),
            ]
            columns = [101, 202]
            data = [
                [0.1, 0.2],
                [-0.1, 0.3],
                [-0.3, 0.0],
            ]
            predictions = pd.DataFrame(data, index=index, columns=columns)
            config["price_interface"] = rtpi
            # Build a Portfolio.
            initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
            portfolio = ottport.get_portfolio_example1(rtpi, initial_timestamp)
            config["portfolio"] = portfolio
            config["order_type"] = "price@twap"
            # Run.
            execution_mode = "batch"
            oplaorde.place_orders(
                predictions,
                execution_mode,
                config,
            )


class TestMarkToMarket1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        # Set up price interface components.
        event_loop = None

        db_txt = """
start_datetime,end_datetime,timestamp_db,price,asset_id
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,107.73,101
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,93.25,202
"""
        db_df = pd.read_csv(
            io.StringIO(db_txt),
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        # Build a ReplayedTimePriceInterface.
        initial_replayed_delay = 5
        delay_in_secs = 0
        sleep_in_secs = 30
        time_out_in_secs = 60 * 5
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        start_datetime = initial_timestamp
        end_datetime = pd.Timestamp("2000-01-01 09:35:00-05:00")
        price_interface = dartttdi.get_replayed_time_price_interface_example1(
            event_loop,
            start_datetime,
            end_datetime,
            initial_replayed_delay,
            delay_in_secs,
            df=db_df,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
        )
        # Initialize portfolio.
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            mark_to_market_col="price",
            timestamp_col="end_datetime",
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
        # Initialize a prediction series.
        predictions = pd.Series(
            index=[101, 202], data=[0.3, -0.1], name="prediction"
        )
        # Mark to market.
        actual = oplaorde.mark_to_market(
            initial_timestamp, predictions, portfolio
        )
        txt = r"""
asset_id,curr_num_shares,prediction,price,value
-1,1000000,0,1,1000000
101,0.0,0.3,107.73,0.0
202,0.0,-0.1,93.25,0.0
"""
        expected = pd.read_csv(
            io.StringIO(txt),
        )
        # Set `asset_id` as index so numpy will type the (remaining) values
        # correctly.
        self.assert_dfs_close(
            actual.set_index("asset_id"),
            expected.set_index("asset_id"),
            rtol=1e-5,
            atol=1e-5,
        )


class TestOptimizeAndUpdate1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        # Set up price interface components.
        event_loop = None

        db_txt = """
start_datetime,end_datetime,timestamp_db,price,asset_id
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,107.73,101
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,93.25,202
2000-01-01 09:35:00-05:00,2000-01-01 09:40:00-05:00,2000-01-01 09:35:00-05:00,108.73,101
2000-01-01 09:35:00-05:00,2000-01-01 09:40:00-05:00,2000-01-01 09:35:00-05:00,93.13,202
"""
        db_df = pd.read_csv(
            io.StringIO(db_txt),
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        # Build a ReplayedTimePriceInterface.
        initial_replayed_delay = 5
        delay_in_secs = 0
        sleep_in_secs = 30
        time_out_in_secs = 60 * 5
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        start_datetime = initial_timestamp
        end_datetime = pd.Timestamp("2000-01-01 09:35:00-05:00")
        price_interface = dartttdi.get_replayed_time_price_interface_example1(
            event_loop,
            start_datetime,
            end_datetime,
            initial_replayed_delay,
            delay_in_secs,
            df=db_df,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
        )
        # Initialize portfolio.
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            mark_to_market_col="price",
            timestamp_col="end_datetime",
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
        # Initialize a prediction series.
        predictions = pd.Series(
            index=[101, 202], data=[0.3, -0.1], name="prediction"
        )
        # Set up order config.
        order_dict_ = {
            "price_interface": price_interface,
            "type_": "price@twap",
            "creation_timestamp": initial_timestamp,
            "start_timestamp": initial_timestamp,
            "end_timestamp": pd.Timestamp("2000-01-01 09:40:00-05:00"),
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        # Mark to market.
        num_orders = oplaorde.optimize_and_update(
            initial_timestamp, predictions, portfolio, order_config
        )
        np.testing.assert_equal(num_orders, 2)
        actual = portfolio.get_characteristics(
            pd.Timestamp("2000-01-01 09:40:00-05:00")
        )
        txt = r"""
,2000-01-01 09:40:00-05:00
net_asset_holdings,50728.36
cash,949271.64
net_wealth,1000000.00
gross_exposure,100664.01
leverage,0.1007
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the csv.
        expected.columns = [pd.Timestamp("2000-01-01 09:40:00-05:00")]
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)
