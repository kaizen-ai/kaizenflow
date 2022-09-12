import unittest.mock as umock

import pandas as pd

import helpers.hunit_test as hunitest
import market_data as mdata
import oms.cc_optimizer_utils as occoputi
import oms.ccxt_broker as occxbrok
import oms.secrets.secret_identifier as oseseide


class TestCcOptimizerUtils1(hunitest.TestCase):
    get_secret_patch = umock.patch.object(occxbrok.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(occxbrok, "ccxt", spec=occxbrok.ccxt)

    @staticmethod
    def get_test_orders(below_min: bool = False):
        """
        Build toy orders for tests.
        """
        df_columns = [
            "asset_id",
            "curr_num_shares",
            "price",
            "position",
            "wall_clock_timestamp",
            "prediction",
            "volatility",
            "spread",
            "target_position",
            "target_notional_trade",
        ]
        if below_min:
            order_df = pd.DataFrame(
                columns=df_columns,
                data=[
                    [
                        8717633868,
                        -1.000,
                        21.696667,
                        -21.696667,
                        pd.Timestamp("2022-09-12 11:06:09.144373-04:00"),
                        -0.133962,
                        0.002366,
                        0,
                        -1.01,
                        -0.01,
                    ],
                    [
                        6051632686,
                        -2.000,
                        5.429500,
                        -10.859000,
                        pd.Timestamp("2022-09-12 11:06:09.144373-04:00"),
                        0.001705,
                        0.002121,
                        0,
                        -2.01,
                        0.01,
                    ],
                ],
            )
        else:
            order_df = pd.DataFrame(
                columns=df_columns,
                data=[
                    [
                        8717633868,
                        -1.000,
                        21.696667,
                        -21.696667,
                        pd.Timestamp("2022-09-12 11:06:09.144373-04:00"),
                        -0.133962,
                        0.002366,
                        0,
                        -27.075329,
                        -5.378662,
                    ],
                    [
                        6051632686,
                        -2.000,
                        5.429500,
                        -10.859000,
                        pd.Timestamp("2022-09-12 11:06:09.144373-04:00"),
                        0.001705,
                        0.002121,
                        0,
                        -33.701572,
                        -22.8425729,
                    ],
                ],
            )
        order_df = order_df.set_index("asset_id")
        return order_df

    @staticmethod
    def get_test_broker() -> occxbrok.CcxtBroker:
        """
        Build `CcxtBroker` for tests.
        """
        universe_version = "v7"
        portfolio_id = "ccxt_portfolio_mock"
        exchange_id = "binance"
        account_type = "trading"
        stage = "preprod"
        contract_type = "futures"
        strategy_id = "dummy_strategy_id"
        market_data = umock.create_autospec(
            spec=mdata.MarketData, instance=True
        )
        secret_id = oseseide.SecretIdentifier(exchange_id, stage, account_type, 1)
        broker = occxbrok.CcxtBroker(
            exchange_id,
            universe_version,
            stage,
            account_type,
            portfolio_id,
            contract_type,
            secret_id,
            strategy_id=strategy_id,
            market_data=market_data
        )
        broker.minimal_order_limits = {8717633868: {'min_amount': 1.0, 'min_cost': 10.0},
                                       6051632686: {'min_amount': 1.0, 'min_cost': 10.0}}
        return broker

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's `start()` method.
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}

    def tearDown(self) -> None:
        self.get_secret_patch.stop()
        self.ccxt_patch.stop()
        # Deallocate in reverse order to avoid race conditions.
        super().tearDown()

    def test_apply_prod_limits1(self) -> None:
        """
        Verify that a correct order is not altered.
        """
        order_df = self.get_test_orders()
        broker = self.get_test_broker()
        with umock.patch.object(
            broker, "get_low_market_price", create=True
        ) as market_price_mock:
            market_price_mock.return_value = 100.0
            # Run.
            actual = occoputi.apply_cc_limits(order_df, broker)
            actual = str(actual)
        self.check_string(actual)

    def test_apply_prod_limits2(self):
        """
        Verify that an order below limit is updated.
        """
        order_df = self.get_test_orders(below_min=True)
        broker = self.get_test_broker()
        with umock.patch.object(
                broker, "get_low_market_price", create=True
        ) as market_price_mock:
            market_price_mock.return_value = 100.0
            # Run.
            actual = occoputi.apply_cc_limits(order_df, broker)
            actual = str(actual)
        self.check_string(actual)
