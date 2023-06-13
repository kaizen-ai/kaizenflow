import pandas as pd
import helpers.hdbg as hdbg
import defi.tulip.implementation.order as dtuimord
# import helpers.hpandas as hpandas


class TestOrder1:
    """
    Check that the Order Class methods are working correctly.
    """
    def test_to_dataframe1(self) -> None:
        timestamp = pd.Timestamp("2023-01-01 00:00:01+00:00")
        base_token = "BTC"
        quote_token = "ETH"
        # Create an order.
        order_1 = dtuimord.Order(
            timestamp=timestamp,
            action="buy",
            quantity=1.2,
            base_token=base_token,
            limit_price=2.1,
            quote_token=quote_token,
            deposit_address=1,
            wallet_address=1,
        )
        # Convert the order to a dataframe.
        order_df = order_1.to_dataframe()
        print(order_df)
        hdbg.dassert_isinstance(order_df, pd.core.frame.DataFrame)