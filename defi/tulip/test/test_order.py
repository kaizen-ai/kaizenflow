import pandas as pd

import defi.tulip.implementation.order as dtuimord


class TestOrder:
    """
    Check that the Order Class methods are working correctly.
    """

    def test_to_dataframe(self) -> None:
        """
        Function to test the to_dataframe() method of the Order class.
        """
        timestamp = pd.Timestamp("2023-01-01 00:00:01+00:00")
        base_token = "BTC"
        quote_token = "ETH"

        # Creating a sample order using 'ddacrord.Order() method'
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

        # Converting the order to a dataframe
        order_df = order_1.to_dataframe()

        assert isinstance(order_df, pd.core.frame.DataFrame)
