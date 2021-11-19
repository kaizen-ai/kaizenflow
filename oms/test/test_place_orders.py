import logging

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import oms.place_orders as oplaorde
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
