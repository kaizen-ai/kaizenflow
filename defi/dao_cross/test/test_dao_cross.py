from typing import List

import pandas as pd

import defi.dao_cross.dao_cross as ddcrdacr
import defi.dao_cross.order as ddacrord
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest


class TestMatchOrders1(hunitest.TestCase):
    def test_match_orders1(self) -> None:
        orders = self._get_test_orders()
        clearing_price = 2
        # Get the actual outcome.
        actual_df = ddcrdacr.match_orders(orders, clearing_price)
        # Check the unique tokens.
        actual_tokens = sorted(list(actual_df["token"].unique()))
        expected_tokens = ["BTC", "ETH"]
        self.assertEqual(actual_tokens, expected_tokens)
        # Check that the DaoCross conservation law is fullfilled.
        btc_quantity = actual_df[actual_df["token"] == "BTC"]["amount"].sum()
        eth_quantity = actual_df[actual_df["token"] == "ETH"]["amount"].sum()
        self.assertEqual(btc_quantity * clearing_price, eth_quantity)
        # Check the signature.
        actual_signature = hpandas.df_to_str(
            actual_df,
            print_shape_info=True,
            tag="df",
        )
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[0, 5]
        columns=token,amount,from,to
        shape=(6, 4)
        token  amount  from  to
        0   BTC     1.2     1   1
        1   ETH     2.4     1   1
        2   BTC     0.3     1   2
        3   ETH     0.6     2   1
        4   BTC     1.1     6   2
        5   ETH     2.2     2   6
        """
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_signature,
            expected_signature,
            dedent=True,
            fuzzy_match=True,
        )

    def _get_test_orders(self) -> List[ddacrord.Order]:
        base_token = "BTC"
        quote_token = "ETH"
        timestamp = pd.Timestamp("2023-01-01 00:00:01+00:00")
        #
        order_1 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="buy",
            quantity=1.2,
            limit_price=2.1,
            deposit_address=1,
            wallet_address=1,
            timestamp=timestamp,
        )
        order_2 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="buy",
            quantity=1.5,
            limit_price=2.3,
            deposit_address=2,
            wallet_address=2,
            timestamp=timestamp + pd.Timedelta("2s"),
        )
        order_3 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.8,
            limit_price=2.1,
            deposit_address=3,
            wallet_address=3,
            timestamp=timestamp + pd.Timedelta("3s"),
        )
        order_4 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="buy",
            quantity=2.1,
            limit_price=0.4,
            deposit_address=4,
            wallet_address=4,
            timestamp=timestamp + pd.Timedelta("4s"),
        )
        order_5 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.5,
            limit_price=0.5,
            deposit_address=1,
            wallet_address=1,
            timestamp=timestamp + pd.Timedelta("5s"),
        )
        order_6 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.1,
            limit_price=0.1,
            deposit_address=6,
            wallet_address=6,
            timestamp=timestamp + pd.Timedelta("6s"),
        )
        orders = [order_1, order_2, order_3, order_4, order_5, order_6]
        return orders
