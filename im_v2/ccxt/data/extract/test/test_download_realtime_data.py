import logging

import helpers.hunit_test as hunitest
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


class TestDownloadRealtimeData(imvcddbut.TestImDbHelper):
    def test_download_realtime_ohlcv(self) -> None:
        """
        Verify that OHLCV data is downloaded in correct format.
        """


class TestDownloadData(hunitest.TestCase):
    class TestInstantiateExchange(hunitest.TestCase):
        def test_instantiate_exchange(self) -> None:
            """
            Verify that exchange tuple is generated correctly.
            """
    def test_download_ohlcv(self) -> None:
        """
        Verify that OHLCV is downloaded in a correct format.
        """

    def test_download_orderbook(self) -> None:
        """
        Verify that orderbook data is downloaded in a correct format.
        """

    def test_download_incorrect(self) -> None:
        """
        Verify that unsupported data formats are handled correctly.
        """