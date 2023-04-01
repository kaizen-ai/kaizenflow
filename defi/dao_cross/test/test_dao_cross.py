from typing import List

import pandas as pd
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

import defi.dao_cross.dao_cross as ddcrdacr
import defi.dao_cross.order as ddacrord


class TestMatchOrders1(hunitest.TestCase):
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
            limit_price=1.1,
            deposit_address=1,
            wallet_address=1,
            timestamp=timestamp,
        )
        order_2 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="buy",
            quantity=1.5,
            limit_price=1.3,
            deposit_address=2,
            wallet_address=2,
            timestamp=timestamp + pd.Timedelta("2s"),
        )
        order_3 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.8,
            limit_price=1.1,
            deposit_address=3,
            wallet_address=3,
            timestamp=timestamp + pd.Timedelta("3s"),
        )
        order_4 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="buy",
            quantity=2.1,
            limit_price=0.9,
            deposit_address=4,
            wallet_address=4,
            timestamp=timestamp + pd.Timedelta("4s"),
        )
        order_5 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.5,
            limit_price=0.9,
            deposit_address=1,
            wallet_address=1,
            timestamp=timestamp + pd.Timedelta("5s"),
        )
        order_6 = ddacrord.Order(
            base_token=base_token,
            quote_token=quote_token,
            action="sell",
            quantity=1.1,
            limit_price=0.8,
            deposit_address=6,
            wallet_address=6,
            timestamp=timestamp + pd.Timedelta("6s"),
        )
        orders = [order_1, order_2, order_3, order_4, order_5, order_6]
        return orders

    def test_match_orders1(self) -> None:
        orders = self._get_test_orders()
        clearing_price = 1
        # Match orders
        actual_df = ddcrdacr.match_orders(orders, clearing_price)
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
        1   ETH     1.2     1   1
        2   BTC     0.3     1   2
        3   ETH     0.3     2   1
        4   BTC     1.1     6   2
        5   ETH     1.1     2   6
        """
        # pylint: enable=line-too-long
        self.assert_equal(
            actual_signature,
            expected_signature,
            dedent=True,
            fuzzy_match=True,
        )