import os

import pandas as pd

import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hunit_test as hunitest
import oms.ccxt_filled_orders as occfiord
import oms.hsecrets as homssec


class TestFilledOrderReader1(hunitest.TestCase):
    _secret_identifier = homssec.SecretIdentifier(
        "binance", "local", "sandbox", 1
    )

    def setUp(self) -> None:
        super().setUp()
        self._root_dir = self.get_scratch_space()

    # TODO(Danya): Factor out common parts.
    def test_read_json_orders1(self) -> None:
        """
        Verify that JSON files are read correctly.
        """
        # Create data.
        data_format = "json"
        self.get_test_data(data_format)
        # Initialize reader.
        reader = occfiord.FilledOrdersReader(
            self._root_dir, self._secret_identifier
        )
        # Choose files for given time period.
        start_ts = pd.Timestamp("2022-09-29T16:00:00", tz="UTC")
        end_ts = pd.Timestamp("2022-10-04T12:00:00", tz="UTC")
        actual = reader.read_json_orders(
            start_ts,
            end_ts,
        )
        # Verify that the number of items in JSON is similar to expected.
        # Note: format of the output is checked instead of contents
        #  due to unordered location of the items in the resulting dictionary.
        self.assertEqual(len(actual), 6)
        # Verify that the items have expected keys.
        expected_keys = {
            "order",
            "price",
            "side",
            "symbol",
            "takerOrMaker",
            "timestamp",
            "type",
            "info",
            "id",
            "fees",
            "fee",
            "datetime",
            "cost",
            "amount",
        }
        actual_keys = set(actual[0].keys())
        self.assertSetEqual(expected_keys, actual_keys)

    def test_read_csv_orders1(self) -> None:
        """
        Verify that CSV files are read correctly.
        """
        # Create data.
        data_format = "csv"
        self.get_test_data(data_format)
        # Initialize reader.
        reader = occfiord.FilledOrdersReader(
            self._root_dir, self._secret_identifier
        )
        # Choose files for given time period.
        start_ts = pd.Timestamp("2022-09-29T16:00:00", tz="UTC")
        end_ts = pd.Timestamp("2022-10-03T17:00:00", tz="UTC")
        actual = reader.read_csv_orders(
            start_ts,
            end_ts,
        )
        actual_str = hpandas.df_to_str(actual)
        self.check_string(actual_str)

    def test_get_file_names_for_time_period1(self) -> None:
        """
        Verify that JSON file names are listed correctly.
        """
        # Create data.
        data_format = "json"
        self.get_test_data(data_format)
        # Initialize reader.
        reader = occfiord.FilledOrdersReader(
            self._root_dir, self._secret_identifier
        )
        # Choose files for given time period.
        start_ts = pd.Timestamp("2022-09-29T16:00:00", tz="UTC")
        end_ts = pd.Timestamp("2022-09-30T16:00:00", tz="UTC")
        actual = reader._get_file_names_for_time_period(
            start_ts, end_ts, data_format
        )
        actual_str = hprint.format_list(actual, sep="\n")
        self.check_string(actual_str, purify_text=True)

    def test_get_file_names_for_time_period2(self) -> None:
        """
        Verify that csv file names are listed correctly.
        """
        # Create data.
        root_dir = self.get_scratch_space()
        data_format = "csv"
        self.get_test_data(data_format)
        # Initialize reader.
        secret_identifier = self._secret_identifier
        reader = occfiord.FilledOrdersReader(root_dir, secret_identifier)
        # Choose files for given time period.
        start_ts = pd.Timestamp("2022-09-29T16:00:00", tz="UTC")
        end_ts = pd.Timestamp("2022-09-30T16:00:00", tz="UTC")
        actual = reader._get_file_names_for_time_period(
            start_ts, end_ts, data_format
        )
        actual_str = hprint.format_list(actual, sep="\n")
        self.check_string(actual_str, purify_text=True)

    def get_test_data(self, data_format: str) -> None:
        if data_format == "json":
            test_json1 = [
                {
                    "info": {
                        "symbol": "APEUSDT",
                        "id": "282773274",
                        "orderId": "5772340563",
                        "side": "SELL",
                        "price": "5.4270",
                        "qty": "5",
                        "realizedPnl": "0",
                        "marginAsset": "USDT",
                        "quoteQty": "27.1350",
                        "commission": "0.01085400",
                        "commissionAsset": "USDT",
                        "time": "1664469999509",
                        "positionSide": "BOTH",
                        "buyer": False,
                        "maker": False,
                    },
                    "timestamp": 1664469999509,
                    "datetime": "2022-09-29T16:46:39.509Z",
                    "symbol": "APE/USDT",
                    "id": "282773274",
                    "order": "5772340563",
                    "type": None,
                    "side": "sell",
                    "takerOrMaker": "taker",
                    "price": 5.427,
                    "amount": 5.0,
                    "cost": 27.135,
                    "fee": {"cost": 0.010854, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.010854}],
                },
                {
                    "info": {
                        "symbol": "APEUSDT",
                        "id": "282775654",
                        "orderId": "5772441841",
                        "side": "BUY",
                        "price": "5.3980",
                        "qty": "6",
                        "realizedPnl": "0.14500000",
                        "marginAsset": "USDT",
                        "quoteQty": "32.3880",
                        "commission": "0.01295520",
                        "commissionAsset": "USDT",
                        "time": "1664470318567",
                        "positionSide": "BOTH",
                        "buyer": True,
                        "maker": False,
                    },
                    "timestamp": 1664470318567,
                    "datetime": "2022-09-29T16:51:58.567Z",
                    "symbol": "APE/USDT",
                    "id": "282775654",
                    "order": "5772441841",
                    "type": None,
                    "side": "buy",
                    "takerOrMaker": "taker",
                    "price": 5.398,
                    "amount": 6.0,
                    "cost": 32.388,
                    "fee": {"cost": 0.0129552, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.0129552}],
                },
                {
                    "info": {
                        "symbol": "APEUSDT",
                        "id": "282779084",
                        "orderId": "5772536135",
                        "side": "BUY",
                        "price": "5.4070",
                        "qty": "3",
                        "realizedPnl": "0",
                        "marginAsset": "USDT",
                        "quoteQty": "16.2210",
                        "commission": "0.00648840",
                        "commissionAsset": "USDT",
                        "time": "1664470620267",
                        "positionSide": "BOTH",
                        "buyer": True,
                        "maker": False,
                    },
                    "timestamp": 1664470620267,
                    "datetime": "2022-09-29T16:57:00.267Z",
                    "symbol": "APE/USDT",
                    "id": "282779084",
                    "order": "5772536135",
                    "type": None,
                    "side": "buy",
                    "takerOrMaker": "taker",
                    "price": 5.407,
                    "amount": 3.0,
                    "cost": 16.221,
                    "fee": {"cost": 0.0064884, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.0064884}],
                },
            ]
            test_json2 = [
                {
                    "info": {
                        "symbol": "AVAXUSDT",
                        "id": "400396688",
                        "orderId": "12598181143",
                        "side": "BUY",
                        "price": "17.3600",
                        "qty": "2",
                        "realizedPnl": "0",
                        "marginAsset": "USDT",
                        "quoteQty": "34.7200",
                        "commission": "0.01388800",
                        "commissionAsset": "USDT",
                        "time": "1664879819754",
                        "positionSide": "BOTH",
                        "buyer": True,
                        "maker": False,
                    },
                    "timestamp": 1664879819754,
                    "datetime": "2022-10-04T10:36:59.754Z",
                    "symbol": "AVAX/USDT",
                    "id": "400396688",
                    "order": "12598181143",
                    "type": None,
                    "side": "buy",
                    "takerOrMaker": "taker",
                    "price": 17.36,
                    "amount": 2.0,
                    "cost": 34.72,
                    "fee": {"cost": 0.013888, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.013888}],
                },
                {
                    "info": {
                        "symbol": "AVAXUSDT",
                        "id": "400396907",
                        "orderId": "12598270086",
                        "side": "SELL",
                        "price": "17.3300",
                        "qty": "7",
                        "realizedPnl": "-0.12000000",
                        "marginAsset": "USDT",
                        "quoteQty": "121.3100",
                        "commission": "0.04852400",
                        "commissionAsset": "USDT",
                        "time": "1664880124173",
                        "positionSide": "BOTH",
                        "buyer": False,
                        "maker": False,
                    },
                    "timestamp": 1664880124173,
                    "datetime": "2022-10-04T10:42:04.173Z",
                    "symbol": "AVAX/USDT",
                    "id": "400396907",
                    "order": "12598270086",
                    "type": None,
                    "side": "sell",
                    "takerOrMaker": "taker",
                    "price": 17.33,
                    "amount": 7.0,
                    "cost": 121.31,
                    "fee": {"cost": 0.048524, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.048524}],
                },
                {
                    "info": {
                        "symbol": "AVAXUSDT",
                        "id": "400397431",
                        "orderId": "12598362254",
                        "side": "BUY",
                        "price": "17.3600",
                        "qty": "6",
                        "realizedPnl": "-0.09000000",
                        "marginAsset": "USDT",
                        "quoteQty": "104.1600",
                        "commission": "0.04166400",
                        "commissionAsset": "USDT",
                        "time": "1664880419818",
                        "positionSide": "BOTH",
                        "buyer": True,
                        "maker": False,
                    },
                    "timestamp": 1664880419818,
                    "datetime": "2022-10-04T10:46:59.818Z",
                    "symbol": "AVAX/USDT",
                    "id": "400397431",
                    "order": "12598362254",
                    "type": None,
                    "side": "buy",
                    "takerOrMaker": "taker",
                    "price": 17.36,
                    "amount": 6.0,
                    "cost": 104.16,
                    "fee": {"cost": 0.041664, "currency": "USDT"},
                    "fees": [{"currency": "USDT", "cost": 0.041664}],
                },
            ]
            # Create JSON scratch directory.
            scratch_json_space = os.path.join(self.get_scratch_space(), "json")
            incremental = False
            hio.create_dir(scratch_json_space, incremental)
            # Save JSON files.
            json1_path = os.path.join(
                scratch_json_space,
                f"fills_20220929-160000_20220929-170000_{self._secret_identifier}.json",
            )
            hio.to_json(json1_path, test_json1)
            json2_path = os.path.join(
                scratch_json_space,
                f"fills_20221004-100000_20221004-110000_{self._secret_identifier}.json",
            )
            hio.to_json(json2_path, test_json2)
        #
        elif data_format == "csv":
            # Generate .csv data.
            csv_columns = [
                "timestamp",
                "symbol",
                "id",
                "order",
                "side",
                "takerOrMaker",
                "price",
                "amount",
                "cost",
                "fees",
                "fees_currency",
                "realized_pnl",
            ]
            test_csv1 = pd.DataFrame(
                data=[
                    [
                        pd.Timestamp("2022-09-29 16:46:39.509000+0000", tz="UTC"),
                        "APE/USDT",
                        282773274,
                        5772340563,
                        "sell",
                        "taker",
                        5.427,
                        5.0,
                        27.135,
                        0.010854,
                        "USDT",
                        0,
                    ],
                    [
                        pd.Timestamp("2022-09-29 16:51:58.567000+0000", tz="UTC"),
                        "APE/USDT",
                        282775654,
                        5772441841,
                        "buy",
                        "taker",
                        5.398,
                        6.0,
                        32.388,
                        0.0129552,
                        "USDT",
                        0.14500000,
                    ],
                    [
                        pd.Timestamp("2022-09-29 16:57:00.267000+0000", tz="UTC"),
                        "APE/USDT",
                        282779084,
                        5772536135,
                        "buy",
                        "taker",
                        5.407,
                        3.0,
                        16.221,
                        0.0064884,
                        "USDT",
                        0,
                    ],
                ],
                columns=csv_columns,
            ).set_index("timestamp")
            test_csv2 = pd.DataFrame(
                data=[
                    [
                        pd.Timestamp("2022-10-04 10:36:59.754000+0000", tz="UTC"),
                        "AVAX/USDT",
                        400396688,
                        12598181143,
                        "buy",
                        "taker",
                        17.36,
                        2.0,
                        34.72,
                        0.013888,
                        "USDT",
                        0,
                    ],
                    [
                        pd.Timestamp("2022-10-04 10:42:04.173000+0000", tz="UTC"),
                        "AVAX/USDT",
                        40039690,
                        12598270086,
                        "sell",
                        "taker",
                        17.33,
                        7.0,
                        121.31,
                        0.048524,
                        "USDT",
                        -0.12000000,
                    ],
                    [
                        pd.Timestamp("2022-10-04 10:46:59.818000+0000", tz="UTC"),
                        "AVAX/USDT",
                        400397431,
                        12598362254,
                        "buy",
                        "taker",
                        17.36,
                        6.0,
                        104.16,
                        0.041664,
                        "USDT",
                        -0.09000000,
                    ],
                ],
                columns=csv_columns,
            ).set_index("timestamp")
            # Create CSV scrach directory.
            scratch_csv_space = os.path.join(self.get_scratch_space(), "csv/")
            incremental = False
            hio.create_dir(scratch_csv_space, incremental)
            # Save tmp CSV files.
            csv1_path = os.path.join(
                scratch_csv_space,
                f"fills_20220929-160000_20220929-170000_{self._secret_identifier}.csv.gz",
            )
            test_csv1.to_csv(csv1_path)
            csv2_path = os.path.join(
                scratch_csv_space,
                f"fills_20221004-100000_20221004-110000_{self._secret_identifier}.csv.gz",
            )
            test_csv2.to_csv(csv2_path)