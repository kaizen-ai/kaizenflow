from typing import Any

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import oms.broker.ccxt.ccxt_execution_quality as obccexqu


def _get_child_order_response_df1() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    timestamps = [
        "2023-03-15 16:35:41.205000+00:00",
        "2023-03-15 16:35:42.205000+00:00",
        "2023-03-15 16:35:43.205000+00:00",
        "2023-03-15 16:35:44.205000+00:00",
    ]
    data = {
        "symbol": ["APE/USDT", "AVAX/USDT ", "APE/USDT", "AVAX/USDT "],
        "asset_id": [6051632686, 8717633868, 6051632686, 8717633868],
        "order": [7954906695, 4551306695, 7909906645, 3154906699],
        "client_order_id": [
            "x-xcKtGhcub89989e55d47273a3610a9",
            "x-xcKtGhcub03933b03766705d9d532f",
            "x-xcKtGhcue1c5f0b2aaddb6ba451d6c",
            "x-xcKtGhcub89981j55d47273a3610a9",
        ],
        "order_type": ["limit", "limit", "limit", "limit"],
        "time_in_force": ["GTC", "GTC", "GTC", "GTC"],
        "post_only": [False, False, False, False],
        "reduce_only": [False, False, False, False],
        "side": ["buy", "sell", "sell", "buy"],
        "order_price": [8.2, 9.85, 2.7, 10.1],
        "stop_price": [None, None, None, None],
        "order_amount": [10, 1, 0, 3],
        "order_update_timestamp": timestamps,
        "order_update_datetime": timestamps,
    }
    df = pd.DataFrame(data)
    df["order_update_datetime"] = pd.to_datetime(df["order_update_datetime"])
    return df


def _get_child_order_df() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    start_ts = [
        pd.Timestamp("2023-03-15 16:35:37.825835+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:38.718960+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:39.718960+0000", tz="UTC"),
        pd.Timestamp("2023-03-15 16:35:40.718960+0000", tz="UTC"),
    ]
    data = {
        "creation_timestamp": start_ts,
        "asset_id": [6051632686, 8717633868, 6051632686, 8717633868],
        "type_": ["limit", "limit", "limit", "limit"],
        "start_timestamp": start_ts,
        "end_timestamp": [
            pd.Timestamp("2023-03-15 16:36:37.825835+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:38.718960+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:39.825835+0000", tz="UTC"),
            pd.Timestamp("2023-03-15 16:36:40.718960+0000", tz="UTC"),
        ],
        "diff_num_shares": [10.0, -1.0, 10, -1],
        "limit_price": [4.12045, 15.804999, 12.88, 10.9],
    }
    df = pd.DataFrame(data)
    return df


def _get_fills_df() -> pd.DataFrame:
    """
    Generate synthetic data.
    """
    timestamps = [
        "2023-03-31 16:35:39.268000+00:00",
        "2023-03-31 16:35:40.268000+00:00",
        "2023-03-31 16:36:41.268000+00:00",
        "2023-03-31 16:36:42.268000+00:00",
    ]
    data = {
        "order": [7954906695, 4551306695, 7909906695, 3154906695],
        "id": [352825168, 762825168, 232825168, 972825168],
        "asset_id": ["6051632686", "8717633868", "6051632686", "8717633868"],
        "symbol": ["APE/USDT", "AVAX/USDT", "APE/USDT", "AVAX/USDT"],
        "buy_count": [1, 1, 0, 0],
        "sell_count": [0, 0, 1, 1],
        "taker_count": [1, 0, 1, 1],
        "maker_count": [0, 1, 0, 0],
        "buy_volume": [10, 1, 0, 0],
        "sell_volume": [0, 0, 12, 5],
        "taker_volume": [10, 0, 12, 5],
        "maker_volume": [0, 1, 0, 0],
        "buy_notional": [42.0, 5.85, 0, 0],
        "sell_notional": [0, 0, 93.6, 19.5],
        "taker_notional": [42.0, 0, 93.6, 19.5],
        "maker_notional": [0, 5.85, 0, 0],
        "price": [4.2, 5.85, 7.8, 3.9],
        "first_timestamp": timestamps,
        "last_timestamp": timestamps,
        "first_datetime": timestamps,
        "last_datetime": timestamps,
        "amount": [10, 1, 12, 5],
        "cost": [42.0, 5.85, 93.6, 19.5],
        "transaction_cost": [0.000009456, 0.0000096, 0.00000612, 0.0000056],
        "realized_pnl": [0, 0, 0, 0],
    }
    df = pd.DataFrame(data)
    datetime_col_names = [
        "first_timestamp",
        "last_timestamp",
        "first_datetime",
        "last_datetime",
    ]
    for col in datetime_col_names:
        df[col] = pd.to_datetime(df[col])
    return df


def _check(self_: Any, actual: pd.DataFrame, expected: str) -> None:
    actual = hpandas.df_to_str(actual, num_rows=None)
    self_.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_limit_order_price(hunitest.TestCase):
    """
    Verify that `get_limit_order_price()` gets limit order prices from orders
    correctly.
    """

    def test1(self) -> None:
        df = _get_child_order_df()
        freq = "T"
        actual = obccexqu.get_limit_order_price(df, freq=freq)
        expected = r"""
                                buy_limit_order_price            sell_limit_order_price
        asset_id                             6051632686 8717633868             6051632686 8717633868
        creation_timestamp
        2023-03-15 16:36:00+00:00              8.500225        NaN                    NaN    13.3525
        """
        _check(self, actual, expected)


class Test_align_ccxt_orders_and_fills(hunitest.TestCase):
    """
    Verify that CCXT orders and fills are aligned correctly.
    """

    def test1(self) -> None:
        child_order_response_df = _get_child_order_response_df1()
        fills_df = _get_fills_df()
        filled_df, unfilled_df = obccexqu.align_ccxt_orders_and_fills(
            child_order_response_df, fills_df
        )
        # Check filled df.
        expected = r"""
                   order_type time_in_force  post_only  reduce_only  side  order_price  stop_price  order_amount            order_update_timestamp             order_update_datetime                   first_timestamp                    last_timestamp                    first_datetime                     last_datetime     symbol    asset_id  buy_count  sell_count  taker_count  maker_count  buy_volume  sell_volume  taker_volume  maker_volume  buy_notional  sell_notional  taker_notional  maker_notional  price  amount   cost  transaction_cost  realized_pnl
        order
        7954906695      limit           GTC      False        False   buy         8.20        None            10  2023-03-15 16:35:41.205000+00:00  2023-03-15 16:35:41.205000+00:00  2023-03-31 16:35:39.268000+00:00  2023-03-31 16:35:39.268000+00:00  2023-03-31 16:35:39.268000+00:00  2023-03-31 16:35:39.268000+00:00   APE/USDT  6051632686          1           0            1            0          10            0            10             0         42.00            0.0            42.0            0.00   4.20      10  42.00          0.000009             0
        4551306695      limit           GTC      False        False  sell         9.85        None             1  2023-03-15 16:35:42.205000+00:00  2023-03-15 16:35:42.205000+00:00  2023-03-31 16:35:40.268000+00:00  2023-03-31 16:35:40.268000+00:00  2023-03-31 16:35:40.268000+00:00  2023-03-31 16:35:40.268000+00:00  AVAX/USDT  8717633868          1           0            0            1           1            0             0             1          5.85            0.0             0.0            5.85   5.85       1   5.85          0.000010             0
        """
        _check(self, filled_df, expected)
        # Check unfilled df.
        expected = r"""
                order_type time_in_force  post_only  reduce_only  side  order_price stop_price  order_amount          order_update_timestamp           order_update_datetime
        order
        3154906699      limit           GTC      False        False   buy         10.1       None             3  2023-03-15 16:35:44.205000+00:00  2023-03-15 16:35:44.205000+00:00
        7909906645      limit           GTC      False        False  sell          2.7       None             0  2023-03-15 16:35:43.205000+00:00  2023-03-15 16:35:43.205000+00:00
        """
        _check(self, unfilled_df, expected)


class Test_compute_filled_order_execution_quality(hunitest.TestCase):
    """
    Verify that filled order execution quality is computed correctly.
    """

    @staticmethod
    def get_fills_df() -> pd.DataFrame:
        """
        Generate synthetic data.
        """
        data = {
            "side": ["buy", "sell"],
            "order_price": [34.12, 15.805],
            "order_amount": [10.0, 7.0],
            "price": [4.2, 30.85],
            "amount": [5, 1],
        }
        idx = [7954906695, 14412582631]
        df = pd.DataFrame(data, index=idx)
        return df

    def test1(self) -> None:
        fills_df = self.get_fills_df()
        tick_decimals = 2
        actual = obccexqu.compute_filled_order_execution_quality(
            fills_df, tick_decimals
        )
        expected = r"""
                    direction  price_improvement_notional  price_improvement_bps  underfill_quantity  underfill_pct  underfill_notional_at_limit_price  underfill_notional_at_transaction_price
        7954906695           1                       29.92            8769.050410                 5.0       0.500000                             170.60                                     21.0
        14412582631         -1                       15.05            9522.303069                 6.0       0.857143                              94.83                                    185.1
        """
        _check(self, actual, expected)


class Test_convert_parent_orders_to_target_position_df(hunitest.TestCase):
    """
    Verify that parent orders are converted into a target position df
    correctly.
    """

    def test1(self) -> None:
        parent_order_df = self._get_parent_orders()
        price_df = self._get_prices()
        target_position_df = obccexqu.convert_parent_orders_to_target_position_df(
            parent_order_df, price_df
        )
        actual = hpandas.df_to_str(target_position_df)
        self.check_string(actual)

    @staticmethod
    def _get_parent_orders() -> pd.DataFrame:
        """
        Generate synthetic parent orders.
        """
        # Create data for the first asset.
        data_asset1 = {
            "order_id": range(5),
            "creation_timestamp": pd.date_range(
                start="2023-08-15 15:45:00",
                end="2023-08-15 16:05:00",
                tz="UTC",
                freq="5T",
            ),
            "asset_id": [1464553467] * 5,
            "type_": ["price@twap"] * 5,
            "start_timestamp": pd.date_range(
                start="2023-08-15 15:45:00",
                end="2023-08-15 16:05:00",
                tz="UTC",
                freq="5T",
            ),
            "end_timestamp": pd.date_range(
                start="2023-08-15 15:50:00",
                end="2023-08-15 16:10:00",
                tz="UTC",
                freq="5T",
            ),
            "curr_num_shares": [0.000, -0.130, -0.046, -0.094, -0.019],
            "diff_num_shares": [
                -0.128214,
                0.107065,
                -0.058212,
                0.124464,
                0.019000,
            ],
            "tz": ["UTC"] * 5,
            "extra_params": [{}] * 5,
        }
        # Create data for the second asset.
        data_asset2 = {
            "order_id": range(5, 10),
            "creation_timestamp": pd.date_range(
                start="2023-08-15 15:45:00",
                end="2023-08-15 16:05:00",
                tz="UTC",
                freq="5T",
            ),
            "asset_id": [1030828978] * 5,
            "type_": ["price@twap"] * 5,
            "start_timestamp": pd.date_range(
                start="2023-08-15 15:45:00",
                end="2023-08-15 16:05:00",
                tz="UTC",
                freq="5T",
            ),
            "end_timestamp": pd.date_range(
                start="2023-08-15 15:50:00",
                end="2023-08-15 16:10:00",
                tz="UTC",
                freq="5T",
            ),
            "curr_num_shares": [0.000, -324.000, -123.000, -771.000, -102.000],
            "diff_num_shares": [
                -402.827474,
                336.926686,
                -1080.730129,
                1116.992220,
                102.000000,
            ],
            "tz": ["UTC"] * 5,
            "extra_params": [{}] * 5,
        }
        # Merge the datasets.
        df_asset1 = pd.DataFrame(data_asset1)
        df_asset2 = pd.DataFrame(data_asset2)
        df = pd.concat([df_asset1, df_asset2]).set_index("order_id")
        return df

    @staticmethod
    def _get_prices() -> pd.DataFrame:
        """
        Generate synthetic price data.
        """
        data = {
            1030828978: [0.20136, 0.20158, 0.20162, 0.20128, 0.20134, 0.20110],
            1464553467: [
                1838.812,
                1839.690,
                1839.936,
                1839.430,
                1840.016,
                1839.615,
            ],
        }
        timestamps = pd.date_range(
            start="2023-08-15 11:45:00",
            end="2023-08-15 12:10:00",
            tz="America/New_York",
            freq="5T",
        )
        df = pd.DataFrame(data, index=timestamps)
        df.index.name = "end_timestamp"
        return df


class Test_convert_bar_fills_to_portfolio_df(hunitest.TestCase):
    """
    Verify that fills, aggregated by bar, are converted into a portfolio df
    correctly.
    """

    def test1(self) -> None:
        fills_df = self._get_fills()
        price_df = self._get_prices()
        portfolio_df = obccexqu.convert_bar_fills_to_portfolio_df(
            fills_df, price_df
        )
        actual = hpandas.df_to_str(portfolio_df)
        self.check_string(actual)

    @staticmethod
    def _get_fills() -> pd.DataFrame:
        """
        Generate synthetic fills.
        """
        # Create data for the first asset.
        data_asset1 = {
            "bar_end_datetime": pd.date_range(
                start="2023-08-15 15:50:00",
                end="2023-08-15 16:10:00",
                tz="UTC",
                freq="5T",
            ),
            "first_timestamp": [pd.Timestamp("2023-08-15 15:46:03.013", tz="UTC")]
            * 5,
            "last_timestamp": [pd.Timestamp("2023-08-15 15:49:03.674", tz="UTC")]
            * 5,
            "first_datetime": [pd.Timestamp("2023-08-15 15:46:03.013", tz="UTC")]
            * 5,
            "last_datetime": [pd.Timestamp("2023-08-15 15:49:03.674", tz="UTC")]
            * 5,
            "symbol": ["BTC/USDT:USDT"] * 5,
            "asset_id": [1467591036] * 5,
            "buy_count": [0, 5, 0, 3, 4],
            "sell_count": [4, 0, 4, 0, 0],
            "taker_count": [3, 1, 0, 0, 0],
            "maker_count": [1, 4, 4, 3, 4],
            "buy_volume": [0.000, 0.005, 0.000, 0.003, 0.008],
            "sell_volume": [0.008, 0.000, 0.008, 0.000, 0.000],
            "taker_volume": [0.006, 0.001, 0.000, 0.000, 0.000],
            "maker_volume": [0.002, 0.004, 0.008, 0.003, 0.008],
            "buy_notional": [0.0000, 146.7151, 0.0000, 87.9862, 234.6194],
            "sell_notional": [234.6916, 0.0000, 234.5468, 0.0000, 0.0000],
            "taker_notional": [176.0144, 29.3378, 0.0000, 0.0000, 0.0000],
            "maker_notional": [58.6772, 117.3773, 234.5468, 87.9862, 234.6194],
            "price": [
                29336.450000,
                29343.020000,
                29318.350000,
                29328.733333,
                29327.425000,
            ],
            "amount": [0.008, 0.005, 0.008, 0.003, 0.008],
            "cost": [234.6916, 146.7151, 234.5468, 87.9862, 234.6194],
            "transaction_cost": [
                0.082141,
                0.035211,
                0.046909,
                0.017597,
                0.046924,
            ],
            "realized_pnl": [0.000000, -0.032850, 0.000000, -0.016341, -0.033109],
        }
        # Create data for the second asset.
        data_asset2 = {
            "bar_end_datetime": pd.date_range(
                start="2023-08-15 15:50:00",
                end="2023-08-15 16:10:00",
                tz="UTC",
                freq="5T",
            ),
            "first_timestamp": [pd.Timestamp("2023-08-15 15:45:27.483", tz="UTC")]
            * 5,
            "last_timestamp": [pd.Timestamp("2023-08-15 15:45:27.483", tz="UTC")]
            * 5,
            "first_datetime": [pd.Timestamp("2023-08-15 15:45:27.483", tz="UTC")]
            * 5,
            "last_datetime": [pd.Timestamp("2023-08-15 15:45:27.483", tz="UTC")]
            * 5,
            "symbol": ["BAKE/USDT:USDT"] * 5,
            "asset_id": [1528092593] * 5,
            "buy_count": [1, 0, 3, 0, 2],
            "sell_count": [0, 4, 0, 2, 0],
            "taker_count": [0, 0, 2, 0, 1],
            "maker_count": [1, 4, 1, 2, 1],
            "buy_volume": [414.000, 0.000, 1068.000, 0.000, 238.000],
            "sell_volume": [0.000, 1056.000, 0.000, 1020.000, 0.000],
            "taker_volume": [0.000, 0.000, 712.000, 0.000, 119.000],
            "maker_volume": [414.000, 1056.000, 356.000, 1020.000, 119.000],
            "buy_notional": [46.8648, 0.0000, 121.1468, 0.0000, 26.8821],
            "sell_notional": [0.0000, 120.1200, 0.0000, 115.5150, 0.0000],
            "taker_notional": [0.0000, 0.0000, 80.7052, 0.0000, 13.4589],
            "maker_notional": [46.8648, 120.1200, 40.4416, 115.5150, 13.4232],
            "price": [0.113200, 0.113750, 0.113433, 0.113250, 0.112950],
            "amount": [414.000, 1056.000, 1068.000, 1020.000, 238.000],
            "cost": [46.8648, 120.1200, 121.1468, 115.5150, 26.8821],
            "transaction_cost": [
                0.009373,
                0.024024,
                0.040370,
                0.023103,
                0.008068,
            ],
            "realized_pnl": [0.000000, 0.192000, 0.189200, -0.007000, 0.062866],
        }
        # Merge the datasets.
        df_asset1 = pd.DataFrame(data_asset1)
        df_asset2 = pd.DataFrame(data_asset2)
        df = (
            pd.concat([df_asset1, df_asset2])
            .set_index("bar_end_datetime")
            .set_index("asset_id", drop=False, append=True)
        ).sort_index()
        return df

    @staticmethod
    def _get_prices() -> pd.DataFrame:
        """
        Generate synthetic price data.
        """
        data = {
            1467591036: [
                29326.26,
                29336.38,
                29338.88,
                29319.54,
                29330.44,
                29321.45,
            ],
            1528092593: [0.11322, 0.11348, 0.11374, 0.11350, 0.11312, 0.11290],
        }
        timestamps = pd.date_range(
            start="2023-08-15 11:45:00",
            end="2023-08-15 12:10:00",
            tz="America/New_York",
            freq="5T",
        )
        df = pd.DataFrame(data, index=timestamps)
        df.index.name = "end_timestamp"
        return df


class Test_generate_fee_summary(hunitest.TestCase):
    """
    Test that fees are summarized correctly.
    """

    @staticmethod
    def get_fills_df() -> pd.DataFrame:
        """
        Generate synthetic data for tests 1, 2.
        """
        data = {
            "transaction_cost": [1000.625, 2134.6375, 33333.25, 6.777],
            "cost": [2134.8, 3333.999, 66666.75, 1000.875],
            "side": ["buy", "sell", "buy", "sell"],
            "takerOrMaker": ["maker", "maker", "maker", "taker"],
            "realized_pnl": [-99.8125, 130.875, 2222.123456, -3000],
        }
        idx = [7954906695, 14412582631, 384398546, 3333019300]
        df = pd.DataFrame(data, index=idx)
        return df

    @staticmethod
    def get_fills_df_2() -> pd.DataFrame:
        """
        Generate synthetic data for tests 3, 4.
        """
        data = {
            "transaction_cost": [1000.625, 2134.6375, 33333.25, 6.777],
            "cost": [2134.8, 3333.999, 66666.75, 1000.875],
            "side": ["buy", "buy", "buy", "buy"],
            "takerOrMaker": ["taker", "taker", "taker", "taker"],
            "realized_pnl": [-99.8125, -130.875, -2222.123456, 3],
        }
        idx = [7954906695, 14412582631, 384398546, 3333019300]
        df = pd.DataFrame(data, index=idx)
        return df

    def test1(self) -> None:
        """
        Test grouping by takerOrMaker to get fee summary.
        """
        df = self.get_fills_df()
        group_by_col = "is_maker"
        actual = obccexqu.generate_fee_summary(df, group_by_col)
        expected = r"""
is_maker                         False          True      combined
fill_count                    1.000000      3.000000      4.000000
traded_volume_dollars      1000.875000  72135.549000  73136.424000
fill_fees_dollars             6.777000  36468.512500  36475.289500
fill_fees_bps                67.710753   5055.553469   4987.294634
realized_pnl_dollars      -3000.000000   2253.185956   -746.814044
realized_pnl_bps         -29973.772949    312.354448   -102.112464
is_buy                        0.000000      2.000000      2.000000
is_maker                      0.000000      3.000000      3.000000
is_positive_realized_pnl      0.000000      2.000000      2.000000
"""
        _check(self, actual, expected)

    def test2(self) -> None:
        """
        Test grouping by takerOrMaker to get fee summary.
        """
        df = self.get_fills_df()
        group_by_col = "is_positive_realized_pnl"
        actual = obccexqu.generate_fee_summary(df, group_by_col)
        expected = r"""
is_positive_realized_pnl        False          True      combined
fill_count                   2.000000      2.000000      4.000000
traded_volume_dollars     3135.675000  70000.749000  73136.424000
fill_fees_dollars         1007.402000  35467.887500  36475.289500
fill_fees_bps             3212.711777   5066.786857   4987.294634
realized_pnl_dollars     -3099.812500   2352.998456   -746.814044
realized_pnl_bps         -9885.630686    336.139040   -102.112464
is_buy                       1.000000      1.000000      2.000000
is_maker                     1.000000      2.000000      3.000000
is_positive_realized_pnl     0.000000      2.000000      2.000000
"""
        _check(self, actual, expected)

    def test3(self) -> None:
        """
        Test grouping by takerOrMaker to get fee summary.
        """
        df = self.get_fills_df_2()
        group_by_col = "is_maker"
        actual = obccexqu.generate_fee_summary(df, group_by_col)
        expected = r"""
is_maker                         False      combined
fill_count                    4.000000      4.000000
traded_volume_dollars     73136.424000  73136.424000
fill_fees_dollars         36475.289500  36475.289500
fill_fees_bps              4987.294634   4987.294634
realized_pnl_dollars      -2449.810956  -2449.810956
realized_pnl_bps           -334.964553   -334.964553
is_buy                        4.000000      4.000000
is_maker                      0.000000      0.000000
is_positive_realized_pnl      1.000000      1.000000
"""
        _check(self, actual, expected)

    def test4(self) -> None:
        """
        Test grouping by takerOrMaker to get fee summary.
        """
        df = self.get_fills_df_2()
        group_by_col = "is_positive_realized_pnl"
        actual = obccexqu.generate_fee_summary(df, group_by_col)
        expected = r"""
is_positive_realized_pnl         False         True      combined
fill_count                    3.000000     1.000000      4.000000
traded_volume_dollars     72135.549000  1000.875000  73136.424000
fill_fees_dollars         36468.512500     6.777000  36475.289500
fill_fees_bps              5055.553469    67.710753   4987.294634
realized_pnl_dollars      -2452.810956     3.000000  -2449.810956
realized_pnl_bps           -340.028043    29.973773   -334.964553
is_buy                        3.000000     1.000000      4.000000
is_maker                      0.000000     0.000000      0.000000
is_positive_realized_pnl      0.000000     1.000000      1.000000
"""
        _check(self, actual, expected)
