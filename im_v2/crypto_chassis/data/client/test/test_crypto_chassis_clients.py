import pandas as pd
import pytest

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce


class TestCryptoChassisHistoricalPqByTileClient1(hunitest.TestCase):
    def test1(self) -> None:
        """
        `dataset = bid_ask`
        `contract_type = futures`
        """
        resample1_min = True
        contract_type = "futures"
        dataset = "bid_ask"
        data_snapshot = "20220620"
        client = imvccdcccce.get_CryptoChassisHistoricalPqByTileClient_example4(
            resample1_min, contract_type, dataset, data_snapshot
        )
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-06-15 00:00:00+00:00")
        end_ts = pd.Timestamp("2022-06-15 16:00:00+00:00")
        columns = None
        filter_data_mode = "assert"
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            filter_data_mode,
        )
        df = hpandas.df_to_str(df)
        expected_signature = r"""
                                full_symbol             bid_price   bid_size     ask_price  ask_size
        timestamp
        2022-06-15 00:00:00+00:00  binance::BTC_USDT  22112.989064    90.241  22108.061867   149.091
        2022-06-15 00:01:00+00:00  binance::BTC_USDT  22063.260453    96.488  22075.549002   100.748
        2022-06-15 00:02:00+00:00  binance::BTC_USDT  22034.594408    65.474  22028.264294   116.809
        ...
        2022-06-15 15:58:00+00:00  binance::BTC_USDT  21395.001921    95.367  21397.577884    69.930
        2022-06-15 15:59:00+00:00  binance::BTC_USDT  21401.457408   110.456  21404.437116    79.228
        2022-06-15 16:00:00+00:00  binance::BTC_USDT  21386.773491    73.926  21394.488583    57.439
        """
        self.assert_equal(df, expected_signature, fuzzy_match=True)

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test2(self) -> None:
        """
        `dataset = ohlcv`
        `contract_type = spot`
        """
        resample1_min = True
        contract_type = "spot"
        dataset = "ohlcv"
        data_snapshot = "20220530"
        client = imvccdcccce.get_CryptoChassisHistoricalPqByTileClient_example4(
            resample1_min, contract_type, dataset, data_snapshot
        )
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-05-15 12:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        columns = ["full_symbol", "open", "high", "low"]
        filter_data_mode = "assert"
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            filter_data_mode,
        )
        df = hpandas.df_to_str(df)
        expected_signature = r"""
                                        full_symbol      open      high       low
        timestamp
        2022-05-15 12:00:00+00:00  binance::BTC_USDT  30241.46  30293.32  30238.39
        2022-05-15 12:01:00+00:00  binance::BTC_USDT  30293.31  30456.36  30291.60
        2022-05-15 12:02:00+00:00  binance::BTC_USDT  30368.16  30393.57  30340.00
        ...
        2022-05-15 15:58:00+00:00  binance::BTC_USDT  29960.20  29976.90  29941.20
        2022-05-15 15:59:00+00:00  binance::BTC_USDT  29976.90  30040.40  29968.33
        2022-05-15 16:00:00+00:00  binance::BTC_USDT  30013.46  30045.63  30007.45
        """

        self.assert_equal(df, expected_signature, fuzzy_match=True)
