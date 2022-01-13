import os

import pytest

import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import im_v2.cryptodatadownload.data.client.cdd_client as imcdaclcd
import im_v2.cryptodatadownload.data.client.cdd_clients_example as imvcrcdcex

_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        exchange_id = "binance"
        currency_pair = "ETH_USDT"
        actual = cdd_client._get_file_path(
            imcdaclcd._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        expected = os.path.join(
            hs3.get_path(),
            "data/cryptodatadownload/20210924/binance/ETH_USDT.csv.gz",
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        exchange_id = "unsupported exchange"
        currency_pair = "ADA_USDT"
        with self.assertRaises(AssertionError):
            cdd_client._get_file_path(
                imcdaclcd._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            cdd_client._get_file_path(
                imcdaclcd._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


class TestCddClient(hunitest.TestCase):
    @pytest.mark.skip("CmapAmp413: 18s. Too slow")
    def test1(self) -> None:
        """
        Test files on S3 are being read correctly.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        actual = cdd_client.read_data("binance::BTC_USDT")
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        with self.assertRaises(AssertionError):
            cdd_client.read_data("unsupported_exchange_id::BTC_USDT")

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        cdd_client = imvcrcdcex.get_CcdClient_example1()
        #
        with self.assertRaises(AssertionError):
            cdd_client.read_data("binance::unsupported_currency_pair")

    def test4(self) -> None:
        """
        Test unsupported data type.
        """
        root_dir = _AM_S3_ROOT_DIR
        data_type = "unsupported_data_type"
        with self.assertRaises(AssertionError):
            cdd_client = imcdaclcd.CddClient(
                data_type, root_dir, aws_profile="am"
            )
