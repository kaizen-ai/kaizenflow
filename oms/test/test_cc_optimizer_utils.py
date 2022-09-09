import unittest.mock as umock

import pandas as pd

import helpers.hunit_test as hunitest
import market_data as mdata
import oms.cc_optimizer_utils as occoputi
import oms.ccxt_broker as occxbrok
import oms.order as omorder
import oms.secrets.secret_identifier as oseseide


class TestCcOptimizerUtils1(hunitest.TestCase):
    get_secret_patch = umock.patch.object(occxbrok.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(occxbrok, "ccxt", spec=occxbrok.ccxt)

    @staticmethod
    def get_test_order(below_min: bool = False):
        """
        Build toy orders for tests.
        """
        if below_min:
            order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
            asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
            end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.001\
            tz=America/New_York\nOrder: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
            asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
            end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121\
            tz=America/New_York"
        else:
            order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
            asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
            end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=10\
            tz=America/New_York\nOrder: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
            asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
            end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=20\
            tz=America/New_York"
        # Get orders.
        orders = omorder.orders_from_string(order_str)
        order_df = pd.DataFrame(
            [omorder.Order.to_dict(order) for order in orders]
        )
        return order_df


        pd.DataFrame({"price": [19256.86, 1.139667], "position": [0, 0],
        })

    @staticmethod
    def get_test_broker() -> occxbrok.CcxtBroker:
        """
        Build `CcxtBroker` for tests.
        """
        exchange_id = "binance"
        universe_version = "v7"
        portfolio_id = "ccxt_portfolio_mock"
        exchange_id = "binance"
        account_type = "trading"
        stage = "preprod"
        contract_type = "futures"
        secret_id = oseseide.SecretIdentifier(exchange_id, stage, account_type, 1)
        broker = occxbrok.CcxtBroker(
            exchange_id,
            universe_version,
            stage,
            account_type,
            portfolio_id,
            contract_type,
            secret_id,
            strategy_id="dummy_strategy_id",
            market_data=umock.create_autospec(
                spec=mdata.MarketData, instance=True
            ),
        )
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
        order_df = self.get_test_order()
        broker = self.get_test_broker()
        low_market_price_patch = umock.patch.object(occoputi.apply_cc_limits, "get_low_market_price")
        low_market_price_patch.return_value = 10.0
        # with umock.patch.object(
        #     broker, "get_low_market_price", create=True
        # ) as fetch_orders_mock:
        #     fetch_orders_mock.return_value = 100.0
        #     # Run.
        actual = occoputi.apply_cc_limits(order_df, broker)
        self.check_string(actual)

    # def test_apply_prod_limits2(self):
    #     """
    #     Verify that an order below limit is updated.
    #     """
    #     ...

    # def test_apply_testnet_limits(self):
    #     """
    #     Verify that an order is altered to have a minimal amount.
    #     """
