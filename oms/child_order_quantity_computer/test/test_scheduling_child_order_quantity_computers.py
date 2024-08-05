import logging
from typing import Dict, List

import pandas as pd

import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import oms.child_order_quantity_computer.dynamic_scheduling_child_order_quantity_computer as ocoqcdscoqc
import oms.child_order_quantity_computer.static_scheduling_child_order_quantity_computer as ocoqcsscoqc
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)


class TestStaticSchedulingChildOrderQuantityComputer(hunitest.TestCase):
    def test_get_wave_quantities1(self) -> None:
        """
        Test that the child order quantities are correctly computed.

        Use two waves, test the first.
        """
        # Define parent orders with different number of shares.
        diff_num_shares = [7.3, 2.5, 1.2]
        num_waves = 2
        wave_id = 0
        wave_quantities = self._get_wave_quantities(
            num_waves, wave_id, diff_num_shares
        )
        # Note that the quantities are rounded to the precision of the asset.
        self.assertDictEqual(wave_quantities, {1: 3.0, 2: 1.2, 3: 0.6})

    def test_get_wave_quantities2(self) -> None:
        """
        Test that the child order quantities are correctly computed.

        Use two waves, test the second.
        """
        # Define parent orders with different number of shares.
        diff_num_shares = [7.3, 2.5, 1.2]
        num_waves = 2
        wave_id = 1
        wave_quantities = self._get_wave_quantities(
            num_waves, wave_id, diff_num_shares
        )
        # Note that the quantities are rounded to the precision of the asset.
        self.assertDictEqual(wave_quantities, {1: 3.0, 2: 1.2, 3: 0.6})

    def test_get_wave_quantities3(self) -> None:
        """
        Test that the child order quantities are correctly computed.

        Use three waves, test the first.
        """
        # Define parent orders with different number of shares.
        diff_num_shares = [4.0, 1.0, 3.0]
        num_waves = 3
        wave_id = 0
        wave_quantities = self._get_wave_quantities(
            num_waves, wave_id, diff_num_shares
        )
        # Note that the quantities are rounded to the precision of the asset.
        self.assertDictEqual(
            wave_quantities,
            {1: 1.0, 2: 0.3, 3: 1.0},
        )

    def test_get_wave_quantities4(self) -> None:
        """
        Test that the child order quantities are correctly computed.

        Use three waves, test the third.
        """
        diff_num_shares = [4.0, 1.0, 3.0]
        num_waves = 3
        wave_id = 2
        wave_quantities = self._get_wave_quantities(
            num_waves, wave_id, diff_num_shares
        )
        # Note that the quantities are rounded to the precision of the asset.
        self.assertDictEqual(
            wave_quantities,
            {1: 1.0, 2: 0.3, 3: 1.0},
        )

    def _get_wave_quantities(
        self, num_waves: int, target_wave_id: int, diff_num_shares: List[float]
    ) -> Dict[int, float]:
        """
        Get next wave quantities.

        :param num_waves: number of waves
        :param target_wave_id: id of the wave to test
        :param diff_num_shares: list of number of shares for each parent
            order
        """
        # Define parent orders with different number of shares.
        parent_orders = _get_parent_orders(diff_num_shares)
        dummy_market_info = _get_market_info()
        # Initialize the scheduler.
        child_order_quantity_computer = (
            ocoqcsscoqc.StaticSchedulingChildOrderQuantityComputer()
        )
        child_order_quantity_computer.set_instance_params(
            parent_orders,
            num_waves,
            dummy_market_info,
        )
        is_first_wave = True
        # We need go through each wave until the target one
        # to get the correct quantities.
        # E.g. if we want to get the quantities for the second wave,
        # we need to go through the first wave, and then get the quantities for the second wave.
        for _ in range(target_wave_id + 1):
            wave_quantities = child_order_quantity_computer.get_wave_quantities(
                is_first_wave,
            )
            is_first_wave = False
        return wave_quantities


class TestDynamicSchedulingChildOrderQuantityComputer(hunitest.TestCase):
    def test_get_wave_quantities1(self) -> None:
        """
        Test that the child order quantities are correctly computed when the
        open positions are empty.
        """
        # Define parent orders with different number of shares.
        diff_num_shares = [4.0, -1.0, 3.0]
        num_waves = 3
        wave_id = 0
        # Define the open positions.
        open_positions = [{}, {}, {}]
        expected_wave_quantities = [
            {1: 4.0, 2: -1.0, 3: 3.0},
            {1: 4.0, 2: -1.0, 3: 3.0},
            {1: 4.0, 2: -1.0, 3: 3.0},
        ]
        # Get the wave quantities.
        self._get_wave_quantities(
            num_waves,
            wave_id,
            diff_num_shares,
            open_positions,
            expected_wave_quantities,
        )

    def test_get_wave_quantities2(self) -> None:
        """
        Test that the child order quantities are correctly computed.
        """
        # Define parent orders with different number of shares.
        diff_num_shares = [4.0, -1.0, 3.0]
        num_waves = 4
        wave_id = 0
        # Define the open positions.
        open_positions = [
            {
                "ETH/USDT:USDT": 0,
                "BTC/USDT:USDT": 0,
            },
            {
                "ETH/USDT:USDT": 2,
                "BTC/USDT:USDT": 0,
            },
            {
                "ETH/USDT:USDT": 3,
                "BTC/USDT:USDT": -1,
            },
            {
                "ETH/USDT:USDT": 3,
                "BTC/USDT:USDT": -1,
            },
        ]
        expected_wave_quantities = [
            {1: 4.0, 2: -1.0, 3: 3.0},
            {1: 2.0, 2: -1.0, 3: 3.0},
            {1: 1.0, 2: 0.0, 3: 3.0},
            {1: 1.0, 2: 0.0, 3: 3.0},
        ]
        # Get the wave quantities.
        self._get_wave_quantities(
            num_waves,
            wave_id,
            diff_num_shares,
            open_positions,
            expected_wave_quantities,
        )

    def test_get_wave_quantities3(self) -> None:
        """
        Test that the child order quantities are correctly computed when
        `wave_id` 0 is skipped.
        """
        diff_num_shares = [4.0, 1.0, 3.0]
        num_waves = 3
        wave_id = 1
        # Define the open positions.
        open_positions = [
            {
                "ETH/USDT:USDT": 2,
                "BTC/USDT:USDT": 0,
            },
            {
                "ETH/USDT:USDT": 3,
                "BTC/USDT:USDT": 1,
            },
        ]
        expected_wave_quantities = [
            {1: 4.0, 2: 1.0, 3: 3.0},
            {1: 3.0, 2: 0.0, 3: 3.0},
        ]
        # Get the wave quantities.
        self._get_wave_quantities(
            num_waves,
            wave_id,
            diff_num_shares,
            open_positions,
            expected_wave_quantities,
        )

    def test_get_wave_quantities4(self) -> None:
        """
        Test that child order quantities are correctly computed when target
        positions is already filled from the previous bar execution.
        """
        diff_num_shares = [4.0, 1.0, 3.0]
        num_waves = 2
        wave_id = 0
        # Define the open positions.
        open_positions = [
            {
                "ETH/USDT:USDT": 2,
                "BTC/USDT:USDT": 0,
            },
            {
                "ETH/USDT:USDT": 3,
                "BTC/USDT:USDT": 1,
            },
        ]
        # Define the target positions. To show that previous target positions
        # do not affect the current bar.
        target_position = {1: 1000.0, 2: -100.0, 3: 3.0}
        expected_wave_quantities = [
            {1: 4.0, 2: 1.0, 3: 3.0},
            {1: 3.0, 2: 0.0, 3: 3.0},
        ]
        # Get the wave quantities.
        self._get_wave_quantities(
            num_waves,
            wave_id,
            diff_num_shares,
            open_positions,
            expected_wave_quantities,
            target_position=target_position,
        )

    def _get_wave_quantities(
        self,
        num_waves: int,
        wave_id: int,
        diff_num_shares: List[float],
        open_positions: List[Dict[str, float]],
        expected_wave_quantities: List[Dict[int, float]],
        *,
        target_position: Dict[int, float] = None,
    ) -> None:
        """
        Get next wave quantities.

        :param num_waves: number of waves
        :param wave_id: id of the wave to test
        :param diff_num_shares: list of number of shares for each parent
            order
        :param open_positions: dict of open positions
        :param expected_wave_quantities: list of expected wave
            quantities
        :param target_position: dict of target positions from previous
            bar
        """
        # Define parent orders with different number of shares.
        parent_orders = _get_parent_orders(diff_num_shares)
        dummy_market_info = _get_market_info()
        # Initialize the scheduler.
        child_order_quantity_computer = (
            ocoqcdscoqc.DynamicSchedulingChildOrderQuantityComputer()
        )
        child_order_quantity_computer.set_instance_params(
            parent_orders,
            num_waves,
            dummy_market_info,
        )
        if target_position:
            child_order_quantity_computer._target_positions = target_position
        is_first_wave = True
        # We need go through each wave until the target one
        # to get the correct quantities.
        # E.g. if we want to get the quantities for the second wave,
        # we need to go through the first wave, and then get the quantities for the second wave.
        while wave_id < num_waves:
            # Update the current positions.
            open_position = open_positions.pop(0)
            child_order_quantity_computer.update_current_positions(open_position)
            # Get wave quantities.
            wave_quantities = child_order_quantity_computer.get_wave_quantities(
                is_first_wave,
            )
            expected_wave_quantitiy = expected_wave_quantities.pop(0)
            msg = hprint.to_str("is_first_wave wave_id open_position")
            # Check the quantities.
            self.assertDictEqual(wave_quantities, expected_wave_quantitiy, msg)
            # Update the wave id.
            wave_id += 1
            is_first_wave = False


def _get_parent_orders(diff_num_shares: List[int]) -> List[oordorde.Order]:
    """
    Generate parent orders.

    :param diff_num_shares: list of number of shares for each parent
        order, e.g. `[1, 2, 3]` for 3 parent orders, 1 share for the
        first, 2 for the second, 3 for the third.
    """
    # Create a parent order with 2 assets and 2 waves.
    creation_timestamp = pd.Timestamp("2022-08-05 09:30:55+00:00")
    start_timestamp = pd.Timestamp("2022-08-05 09:31:00+00:00")
    end_timestamp = pd.Timestamp("2022-08-05 09:32:00+00:00")
    orders_str = "\n".join(
        [
            f"Order: order_id=1 creation_timestamp={creation_timestamp} asset_id=1464553467 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares=0 diff_num_shares={diff_num_shares[0]} tz=UTC extra_params={{'ccxt_symbol': 'ETH/USDT:USDT'}}",
            f"Order: order_id=2 creation_timestamp={creation_timestamp} asset_id=1464553468 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares=0 diff_num_shares={diff_num_shares[1]} tz=UTC extra_params={{'ccxt_symbol': 'BTC/USDT:USDT'}}",
            f"Order: order_id=3 creation_timestamp={creation_timestamp} asset_id=1464553469 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares=0 diff_num_shares={diff_num_shares[2]} tz=UTC extra_params={{'ccxt_symbol': 'XPR/USDT:USDT'}}",
        ]
    )
    parent_orders = oordorde.orders_from_string(orders_str)
    return parent_orders


def _get_market_info() -> Dict[int, Dict[str, int]]:
    """
    Generate market info for the parent orders.
    """
    dummy_market_info = {
        1464553467: {
            "amount_precision": 0,
            "price_precision": 3,
            "max_leverage": 1,
        },
        1464553468: {
            "amount_precision": 1,
            "price_precision": 3,
            "max_leverage": 1,
        },
        1464553469: {
            "amount_precision": 3,
            "price_precision": 3,
            "max_leverage": 1,
        },
    }
    return dummy_market_info
