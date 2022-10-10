import logging

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.broker as ombroker
import oms.order as omorder
import oms.portfolio as omportfo
import oms.target_position_and_order_generator_example as otpaogeex

_LOG = logging.getLogger(__name__)


class TestTargetPositionAndOrderGenerator1(hunitest.TestCase):
    @staticmethod
    def reset() -> None:
        ombroker.Fill._fill_id = 0
        omorder.Order._order_id = 0

    def get_target_position_and_order_generator1(self) -> omportfo.Portfolio:
        self.reset()
        with hasynci.solipsism_context() as event_loop:
            (
                market_data,
                _,
            ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
            target_position_and_order_generator = (
                otpaogeex.get_TargetPositionAndOrderGenerator_example1(
                    event_loop, market_data
                )
            )
        return target_position_and_order_generator

    def setUp(self) -> None:
        super().setUp()
        self.reset()

    def tearDown(self) -> None:
        super().tearDown()
        self.reset()

    def test_compute(self) -> None:
        expected = r"""
<oms.target_position_and_order_generator.TargetPositionAndOrderGenerator at 0x>
# last target positions=
          holdings_shares        price  holdings_notional wall_clock_timestamp  prediction  volatility  spread  target_holdings_notional  target_trades_notional  target_trades_shares  target_holdings_shares
asset_id
101                     0  1000.311925                  0                  0.0           1      0.0001    0.01             100031.192549           100031.192549                 100.0                   100.0
# last orders=
Order: order_id=0 creation_timestamp=2000-01-01 09:35:00-05:00 asset_id=101 type_=price@twap start_timestamp=2000-01-01 09:35:00-05:00 end_timestamp=2000-01-01 09:40:00-05:00 curr_num_shares=0.0 diff_num_shares=100.0 tz=America/New_York
"""
        target_position_and_order_generator = (
            self.get_target_position_and_order_generator1()
        )
        predictions = pd.Series([1], [101], name="prediction")
        volatility = pd.Series([0.0001], [101], name="volatility")
        spread = pd.Series([0.01], [101], name="spread")
        liquidate_holdings = False
        orders = target_position_and_order_generator.compute_target_positions_and_generate_orders(
            predictions,
            volatility,
            spread,
            liquidate_holdings,
        )
        _ = orders
        _LOG.debug(
            "target_position_and_order_generator=%s",
            target_position_and_order_generator,
        )
        self.assert_equal(
            str(target_position_and_order_generator),
            expected,
            purify_text=True,
            fuzzy_match=True,
        )