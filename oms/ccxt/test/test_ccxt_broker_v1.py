import asyncio
import logging
import os
import pprint
import unittest.mock as umock
from typing import Type

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hio as hio
import helpers.hpandas as hpandas
import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_broker_test_case as occbteca
import oms.ccxt.ccxt_broker_v1 as occcbrv1

_LOG = logging.getLogger(__name__)

# #############################################################################
# TestCcxtBroker_V1
# #############################################################################


class TestCcxtBroker_V1(occbteca.TestCcxtBroker_TestCase):
    def get_broker_class(self) -> Type[ocabccbr.AbstractCcxtBroker]:
        """
        Return class used to instantiate CCXT Broker.
        """
        return occcbrv1.CcxtBroker_v1

    def test_submit_orders1(
        self,
    ) -> None:
        """
        Verify that order is properly submitted via mocked exchange.
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
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            create_order_mock.side_effect = [{"id": 0}]
            orders = occbteca._get_test_order("market")
            broker.market_data.get_wall_clock_time.return_value = pd.Timestamp(
                "2022-08-05 10:37:00-04:00", tz="America/New_York"
            )
            # Run.
            receipt, order_df = asyncio.run(
                broker._submit_orders(orders, "dummy_timestamp", dry_run=False)
            )
            submitted_orders = broker._previous_parent_orders
        # Check the count of calls.
        self.assertEqual(create_order_mock.call_count, 1)
        # Check the args.
        actual_args = pprint.pformat(tuple(create_order_mock.call_args))
        expected_args = r"""
            ((),
            {'amount': 0.121,
            'params': {'client_oid': 0, 'portfolio_id': 'ccxt_portfolio_mock'},
            'side': 'buy',
            'symbol': 'ETH/USDT',
            'type': 'market'})
        """
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        # Check the receipt.
        self.assert_equal(receipt, "order_0")
        # Check the order Dataframe.
        act = hpandas.convert_df_to_json_string(order_df, n_tail=None)
        exp = r"""
            original shape=(1, 10)
            Head:
            {
                "0":{
                    "order_id":0,
                    "creation_timestamp":"2022-08-05T14:36:44Z",
                    "asset_id":1464553467,
                    "type_":"market",
                    "start_timestamp":"2022-08-05T14:36:44Z",
                    "end_timestamp":"2022-08-05T14:38:44Z",
                    "curr_num_shares":0.0,
                    "diff_num_shares":0.121,
                    "tz":"America\/New_York",
                    "extra_params":{
                        "max_leverage":1,
                        "submit_single_order_to_ccxt.start.timestamp":"2022-08-05T14:37:00Z",
                        "ccxt_id":0,
                        "submit_single_order_to_ccxt.num_attempts":0,
                        "submit_single_order_to_ccxt.end.timestamp":"2022-08-05T14:37:00Z"
                    }
                }
            }
            Tail:
        """
        self.assert_equal(act, exp, fuzzy_match=True)
        submitted_orders = pprint.pformat(submitted_orders)
        exp = r"""
        [Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=1464553467 type_=market start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121 tz=America/New_York extra_params={'max_leverage': 1, 'submit_single_order_to_ccxt.start.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'), 'ccxt_id': 0, 'submit_single_order_to_ccxt.num_attempts': 0, 'submit_single_order_to_ccxt.end.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York')}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)

    # TODO(Juraj): Refactor to use _test_submit_orders and test all order types.
    # def test_submit_orders1(
    #    self,
    # ) -> None:
    #    """
    #    Verify that orders are properly submitted via mocked exchange.
    #    """
    #    # Build broker.
    #    universe_version = "v5"
    #    account_type = "trading"
    #    contract_type = "spot"
    #    secret_id = 1
    #    bid_ask_im_client = None
    #    broker = self._get_test_broker(
    #        universe_version,
    #        account_type,
    #        contract_type,
    #        secret_id,
    #        bid_ask_im_client,
    #    )
    #        # Patch main external source.
    #    with umock.patch.object(
    #        broker._exchange, "createOrder", create=True
    #    ), umock.patch.object(
    #        broker._exchange, "createOrder", create=True
    #    )  as create_order_mock:
    #        create_order_mock.side_effect = [{"id": 0}, {"id": None}]
    #        orders = self._get_test_orders2()
    #        # Run.
    #        receipt, order_df = asyncio.run(
    #            broker._submit_orders(
    #                orders, "dummy_timestamp", dry_run=False
    #            )
    #        )
    #        submitted_orders = broker._previous_parent_orders
    #    # Check the count of calls.
    #    self.assertEqual(create_order_mock.call_count, 2)
    #    # Check the args.
    #    actual_args = pprint.pformat(tuple(create_order_mock.call_args))
    #    expected_args = r"""
    #        ((),
    #        {'amount': 0.121,
    #        'params': {'client_oid': 0, 'portfolio_id': 'ccxt_portfolio_mock'},
    #        'side': 'buy',
    #        'symbol': 'ETH/USDT',
    #        'type': 'market'})
    #    """
    #    self.assert_equal(actual_args, expected_args, fuzzy_match=True)
    #    # Check the receipt.
    #    self.assert_equal(receipt, "order_0")
    #    # Check the order Dataframe.
    #    act = hpandas.convert_df_to_json_string(order_df, n_tail=None)
    #    exp = r"""
    #        original shape=(1, 10)
    #        Head:
    #        {
    #            "0":{
    #                "order_id":0,
    #                "creation_timestamp":"2022-08-05T10:36:00Z",
    #                "asset_id":1464553467,
    #                "type_":"limit",
    #                "start_timestamp":"2022-08-05T10:36:00Z",
    #                "end_timestamp":"2022-08-05T10:39:00Z",
    #                "curr_num_shares":2500.0,
    #                "diff_num_shares":10.0,
    #                "tz":"America\/New_York",
    #                "extra_params":{
    #                    "ccxt_id":0
    #                }
    #            }
    #        }
    #        Tail:
    #    """
    #    self.assert_equal(act, exp, fuzzy_match=True)
    #    submitted_orders = pprint.pformat(submitted_orders)
    #    exp = r"""
    #    Order: order_id=0 creation_timestamp=2022-08-05 10:36:00-04:00 asset_id=1464553467 type_=limit start_timestamp=2022-08-05 10:36:00-04:00 end_timestamp=2022-08-05 10:39:00-04:00 curr_num_shares=2500.0 diff_num_shares=10.0 tz=America/New_York extra_params={'ccxt_id': [0]}
    #    """
    #    self.assert_equal(submitted_orders, exp, fuzzy_match=True)

    @umock.patch.object(ocabccbr.asyncio, "sleep")
    def test_submit_orders_errors2(self, sleep_mock: umock.MagicMock) -> None:
        """
        Verify that Binance API error is raised correctly.
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
        broker._submitted_order_id = 0
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            # Check the Binance API error.
            from ccxt.base.errors import ExchangeNotAvailable

            create_order_mock.side_effect = [
                ExchangeNotAvailable(umock.Mock(), ""),
                Exception("MockException"),
            ]
            orders = occbteca._get_test_order("price@twap")
            with umock.patch.object(
                self.abstract_ccxt_mock,
                "ExchangeNotAvailable",
                ExchangeNotAvailable,
            ):
                # Run.
                with self.assertRaises(Exception) as cm:
                    asyncio.run(
                        broker._submit_orders(
                            orders, "dummy_timestamp", dry_run=False
                        )
                    )
        self.assert_equal(str(cm.exception), "MockException")
        # Check the count of calls.
        self.assertEqual(sleep_mock.call_count, 1)

    def test_submit_orders_errors3(self) -> None:
        """
        Verify that the order is not submitted in case of a liquidity error.
        """
        orders = occbteca._get_test_order("price@twap")
        # Define broker parameters.
        universe_version = "v5"
        account_type = "trading"
        contract_type = "spot"
        secret_id = 1
        bid_ask_im_client = None
        # Initialize class.
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            # Check that the order is not submitted if the error is connected to liquidity.
            from ccxt.base.errors import ExchangeNotAvailable, OrderNotFillable

            create_order_mock.side_effect = [
                OrderNotFillable(umock.Mock(status=-4131), '"code":-4131,')
            ]
            with umock.patch.object(
                self.abstract_ccxt_mock,
                "ExchangeNotAvailable",
                ExchangeNotAvailable,
            ):
                # Run.
                _, order_df = asyncio.run(
                    broker._submit_orders(
                        orders, "dummy_timestamp", dry_run=False
                    )
                )
        # Order df should be empty.
        self.assertEqual(order_df.empty, True)

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
            broker._exchange, "fetchMyTrades", create=True
        ) as fetch_trades_mock:
            # Load an example of a return value.
            # Command to generate:
            #  oms/ccxt/scripts/get_ccxt_trades.py \
            #  --start_timestamp 2022-09-01T00:00:00.000Z \
            #  --end_timestamp 2022-09-01T00:10:00.000Z \
            #  --dst_dir oms/ccxt/test/outcomes/TestCcxtBroker_TestCase.test_get_ccxt_trades_for_time_period/input/ \
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
            broker._exchange, "fetchPositions", create=True
        ) as fetch_positions_mock, umock.patch.object(
            broker._exchange, "cancelAllOrders", create=True
        ) as cancel_orders_mock, umock.patch.object(
            broker._exchange, "createLimitBuyOrder", create=True
        ) as limit_buy_order_mock, umock.patch.object(
            broker._exchange, "createLimitSellOrder", create=True
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
        # TODO(Juraj): since this is correlated with the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        # TODO(Juraj, Danya): check if the expected string is correct.
        exp = r"""
        [Order: order_id=0 creation_timestamp=2022-08-05 10:35:55-04:00 asset_id=1464553467 type_=limit start_timestamp=2022-08-05 10:35:55-04:00 end_timestamp=2022-08-05 10:36:55-04:00 curr_num_shares=2500.0 diff_num_shares=5.0 tz=America/New_York extra_params={'max_leverage': 1, 'submit_single_order_to_ccxt.start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'), 'ccxt_id': 0, 'submit_single_order_to_ccxt.num_attempts': 0, 'submit_single_order_to_ccxt.end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')},
        Order: order_id=1 creation_timestamp=2022-08-05 10:35:55-04:00 asset_id=1467591036 type_=limit start_timestamp=2022-08-05 10:35:55-04:00 end_timestamp=2022-08-05 10:36:55-04:00 curr_num_shares=1000.0 diff_num_shares=-10.0 tz=America/New_York extra_params={'max_leverage': 1, 'submit_single_order_to_ccxt.start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'), 'ccxt_id': 2, 'submit_single_order_to_ccxt.num_attempts': 0, 'submit_single_order_to_ccxt.end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')},
        Order: order_id=2 creation_timestamp=2022-08-05 10:35:55-04:00 asset_id=1464553467 type_=limit start_timestamp=2022-08-05 10:35:55-04:00 end_timestamp=2022-08-05 10:36:55-04:00 curr_num_shares=2500.0 diff_num_shares=5.0 tz=America/New_York extra_params={'max_leverage': 1, 'submit_single_order_to_ccxt.start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'), 'ccxt_id': 1, 'submit_single_order_to_ccxt.num_attempts': 0, 'submit_single_order_to_ccxt.end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')},
        Order: order_id=3 creation_timestamp=2022-08-05 10:35:55-04:00 asset_id=1467591036 type_=limit start_timestamp=2022-08-05 10:35:55-04:00 end_timestamp=2022-08-05 10:36:55-04:00 curr_num_shares=1000.0 diff_num_shares=-10.0 tz=America/New_York extra_params={'max_leverage': 1, 'submit_single_order_to_ccxt.start.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'), 'ccxt_id': 3, 'submit_single_order_to_ccxt.num_attempts': 0, 'submit_single_order_to_ccxt.end.timestamp': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}]
        """
        actual_orders = pprint.pformat(orders)
        self.assert_equal(actual_orders, exp, fuzzy_match=True)
        # TODO(Grisha): test also when there are no ccxt_ids for the parent order.
        submitted_orders = pprint.pformat(submitted_orders)
        # TODO(Grisha): convert to a df and freeze instead of using string.
        # TODO(Juraj): since this is almost the same as the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        exp = r"""
        [Order: order_id=0 creation_timestamp=2022-08-05 10:36:00-04:00 asset_id=1464553467 type_=limit start_timestamp=2022-08-05 10:36:00-04:00 end_timestamp=2022-08-05 10:39:00-04:00 curr_num_shares=2500.0 diff_num_shares=10.0 tz=America/New_York extra_params={'ccxt_id': [0, 1]},
        Order: order_id=1 creation_timestamp=2022-08-05 10:36:00-04:00 asset_id=1467591036 type_=limit start_timestamp=2022-08-05 10:36:00-04:00 end_timestamp=2022-08-05 10:39:00-04:00 curr_num_shares=1000.0 diff_num_shares=-20.0 tz=America/New_York extra_params={'ccxt_id': [2, 3]}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)
