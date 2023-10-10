import asyncio
import logging
import os
import pprint
import pytest
import unittest.mock as umock
from typing import Type

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hio as hio
import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
import oms.broker.ccxt.test.ccxt_broker_test_case as obccbteca
import oms.broker.ccxt.ccxt_broker_v1 as obccbrv1

_LOG = logging.getLogger(__name__)

# #############################################################################
# TestCcxtBroker_V1
# #############################################################################

@pytest.mark.skip(reason="Skipped to work in follow-ups, test will be reenabled in CmTask4852")
class TestCcxtBroker_V1(obccbteca.TestCcxtBroker_TestCase):
    def get_broker_class(self) -> Type[obcaccbr.AbstractCcxtBroker]:
        """
        Return class used to instantiate CCXT Broker.
        """
        return obccbrv1.CcxtBroker_v1

    def test_get_ccxt_trades_for_time_period(self) -> None:
        """
        Verify that conducted CCXT trades are requested properly.
        """
        # Build broker.
        universe_version = "v5"
        account_type = "trading"
        contract_type = "spot"
        secret_id = 1
        bid_ask_im_client = None
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        #
        with umock.patch.object(
            broker._async_exchange, "fetchMyTrades", create=True
        ) as fetch_trades_mock:
            # Load an example of a return value.
            # Command to generate:
            #  oms/broker/ccxt/scripts/get_ccxt_trades.py \
            #  --start_timestamp 2022-09-01T00:00:00.000Z \
            #  --end_timestamp 2022-09-01T00:10:00.000Z \
            #  --dst_dir oms/broker/ccxt/test/outcomes/TestCcxtBroker_TestCase.test_get_ccxt_trades_for_time_period/input/ \
            #  --exchange_id binance \
            #  --contract_type futures \
            #  --stage preprod \
            #  --account_type trading \
            #  --secrets_id 1 \
            #  --universe v5
            # Note: the return value is generated via the script rather than
            #  the mocked CCXT function, so the test covers only the format of
            #  the data, and not the content.
            return_value_path = os.path.join(
                self.get_input_dir(test_class_name=self.get_class_name()),
                "trades.json",
            )
            fetch_trades_mock.return_value = hio.from_json(return_value_path)
            start_timestamp = pd.Timestamp("2022-09-01T00:00:00.000Z")
            end_timestamp = pd.Timestamp("2022-09-01T00:10:00.000Z")
            trades = broker._get_ccxt_trades_for_time_period(
                start_timestamp, end_timestamp
            )
            # Verify that the format of the data output is correct.
            self.assertEqual(len(trades), 774)
            self.assertIsInstance(trades, list)
            self.assertIsInstance(trades[0], dict)
            # Verify that trades are fetched for each asset in the universe.
            self.assertEqual(fetch_trades_mock.call_count, 9)
            # Verify that the arguments are called correctly.
            expected_calls = [
                umock.call(
                    symbol="ADA/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="AVAX/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="BNB/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="BTC/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="DOGE/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="EOS/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="ETH/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="LINK/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="SOL/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
            ]
            fetch_trades_mock.assert_has_calls(expected_calls)

    # TODO(Juraj, Danya): ensure, the test is running correctly, the run time is only 0.2s.
    # @pytest.mark.slow("~15 seconds.")
    # TODO(Juraj): Create similiar test for broker v2 as well.
    def test_submit_twap_orders1(self) -> None:
        """
        Verify that a TWAP order is submitted correctly.
        """
        # Build broker.
        universe_version = "v7"
        account_type = "trading"
        contract_type = "futures"
        secret_id = 1
        bid_ask_im_client = self._get_mock_bid_ask_im_client()
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        # Mock any interaction with the exchange.
        with umock.patch.object(
            broker._async_exchange, "fetchPositions", create=True
        ) as fetch_positions_mock, umock.patch.object(
            broker._async_exchange, "cancelAllOrders", create=True
        ) as cancel_orders_mock, umock.patch.object(
            broker._async_exchange, "createLimitBuyOrder", create=True
        ) as limit_buy_order_mock, umock.patch.object(
            broker._async_exchange, "createLimitSellOrder", create=True
        ) as limit_sell_order_mock:
            # Set the wall clock.
            # TODO(Grisha): consider using the UTC timezone because
            # exchanges operate in UTC.
            broker.market_data.get_wall_clock_time.return_value = pd.Timestamp(
                "2022-08-05 10:35:55-04:00", tz="America/New_York"
            )
            # Set fixed value for exchange position amount.
            fetch_positions_mock.return_value = [
                {"info": {"positionAmt": 2500}, "symbol": "ETH/USDT"},
                {"info": {"positionAmt": 1000}, "symbol": "BTC/USDT"},
            ]
            # Set order IDs.
            limit_buy_order_mock.side_effect = [{"id": 0}, {"id": 1}]
            limit_sell_order_mock.side_effect = [{"id": 2}, {"id": 3}]
            # Create and submit TWAP orders.
            orders = self._get_test_orders2()
            passivity_factor = 0.4
            execution_freq = "1T"
            coroutines = [
                broker.submit_twap_orders(
                    orders, passivity_factor, execution_freq=execution_freq
                )
            ]
            with hasynci.solipsism_context() as event_loop:
                orders = hasynci.run(
                    asyncio.gather(*coroutines), event_loop=event_loop
                )[0]
            submitted_orders = broker._previous_parent_orders
        # Check the count of calls.
        self.assertEqual(limit_sell_order_mock.call_count, 2)
        self.assertEqual(limit_buy_order_mock.call_count, 2)
        # Check the order Dataframe.
        # TODO(Juraj): since this is correlated with the generated input, it
        # should not be hardcoded, instead it should be made programatically
        # possible to generate the expected output.
        # TODO(Juraj, Danya): check if the expected string is correct.
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:35:55-04:00
        end_timestamp=2022-08-05 10:36:55-04:00
        curr_num_shares=2500.0
        diff_num_shares=5.0
        tz=America/New_York
        extra_params={'ccxt_id': [0],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}},
        Order:
        order_id=1
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:35:55-04:00
        end_timestamp=2022-08-05 10:36:55-04:00
        curr_num_shares=1000.0
        diff_num_shares=-10.0
        tz=America/New_York
        extra_params={'ccxt_id': [2],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}},
        Order:
        order_id=2
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:35:55-04:00
        end_timestamp=2022-08-05 10:36:55-04:00
        curr_num_shares=2500.0
        diff_num_shares=5.0
        tz=America/New_York
        extra_params={'ccxt_id': [1],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}},
        Order:
        order_id=3
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:35:55-04:00
        end_timestamp=2022-08-05 10:36:55-04:00
        curr_num_shares=1000.0
        diff_num_shares=-10.0
        tz=America/New_York
        extra_params={'ccxt_id': [3],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}}]
        """
        actual_orders = pprint.pformat(orders)
        self.assert_equal(actual_orders, exp, fuzzy_match=True)
        # TODO(Grisha): test also when there are no ccxt_ids for the parent order.
        submitted_orders = pprint.pformat(submitted_orders)
        # TODO(Grisha): convert to a df and freeze instead of using string.
        # TODO(Juraj): since this is almost the same as the generated input, it
        # should not be hardcoded, instead it should be made programatically
        # possible to generate the expected output.
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 10:36:00-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:36:00-04:00
        end_timestamp=2022-08-05 10:39:00-04:00
        curr_num_shares=2500.0
        diff_num_shares=10.0
        tz=America/New_York
        extra_params={'ccxt_id': [0, 1]},
        Order:
        order_id=1
        creation_timestamp=2022-08-05 10:36:00-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:36:00-04:00
        end_timestamp=2022-08-05 10:39:00-04:00
        curr_num_shares=1000.0
        diff_num_shares=-20.0
        tz=America/New_York
        extra_params={'ccxt_id': [2, 3]}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)
