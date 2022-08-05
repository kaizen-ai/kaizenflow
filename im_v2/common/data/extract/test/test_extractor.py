from typing import Any

import pandas as pd
import pytest

import helpers.hunit_test as hunitest
import im_v2.common.data.extract.extractor as imvcdexex


class DummyExtractor(imvcdexex.Extractor):
    """
    Abstract methods can not be mocked or tested other way.
    """
    # Placeholder variables to be used for comparison.
    exchange_id = None
    currency_pair = None
    kwargs = None

    def _download_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        assert self.exchange_id == exchange_id
        assert self.currency_pair == currency_pair
        assert self.kwargs == kwargs

    def _download_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        assert self.exchange_id == exchange_id
        assert self.currency_pair == currency_pair
        assert self.kwargs == kwargs

    def _download_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        assert self.exchange_id == exchange_id
        assert self.currency_pair == currency_pair
        assert self.kwargs == kwargs


class TestDownloadHistoricalData1(hunitest.TestCase):
    def test_download_data1(self) -> None:
        """
        Verify if proper function is called depending on `data_type`.
        """
        dummy_extractor = DummyExtractor()
        test_samples = [
            ("ohlcv", "dummy_id", "dummy_currency_pair", {"type": "ohlcv"}),
            ("bid_ask", "dummy_id", "dummy_currency_pair", {"type": "bid_ask"}),
            ("trades", "dummy_id", "dummy_currency_pair", {"type": "trades"}),
        ]
        for data_type, exchange_id, currency_pair, kwargs in test_samples:
            dummy_extractor.exchange_id = exchange_id
            dummy_extractor.currency_pair = currency_pair
            dummy_extractor.kwargs = kwargs
            # Run.
            dummy_extractor.download_data(
                data_type, exchange_id, currency_pair, **kwargs
            )

    def test_download_data_error1(self) -> None:
        """
        Verify that error is raised on unknown `data_type`.
        """
        dummy_extractor = DummyExtractor()
        with pytest.raises(AssertionError) as fail:
            # Run.
            dummy_extractor.download_data("dummy_data_type", "", "")
        actual_error = str(fail.value)
        expected_error = r"""
            Unknown data type dummy_data_type. Possible data types: ohlcv, bid_ask, trades
        """
        self.assert_equal(actual_error, expected_error, fuzzy_match=True)
