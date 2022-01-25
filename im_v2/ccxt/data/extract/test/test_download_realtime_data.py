import logging
import helpers.hunit_test as hunitest
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


class TestDownloadRealtimeData(imvcddbut.TestImDbHelper):
    def test_download_realtime_ohlcv_to_db(self) -> None:
        """
        Verify that OHLCV data is downloaded in correct format.
        """
        # TODO(steysie): Check the parent class and its uses for extra methods.
        #  Tests should check writing to temporary DB (implemented in the class).
        pass

    def test_download_realtime_ohlcv_to_disk(self) -> None:
        """
        Verify that OHLCV data is downloaded in correct format.
        """
        # TODO(steysie): Check the parent class for extra methods.
        #  Tests should check writing to scratch space (.get_scratch_space()) method.
        #  This is a different script call from previous method: previous saves to DB only,
        #  this one to disk as well. Use `--universe "small"` for both.
        pass


class TestDownloadData(hunitest.TestCase):
        def test_download_ohlcv(self) -> None:
            """
            Verify that OHLCV is downloaded in a correct format.
            """
            # TODO(steysie): Provide fixed input parameters, instantiate exchange,
            #  drop `created_at` column, convert to json and use check_string.
            pass

        def test_download_orderbook(self) -> None:
            """
            Verify that orderbook data is downloaded in a correct format.
            """
            # TODO(steysie): Note that there is no historical data.
            #  Check data type and structure, not contents.
            pass

        def test_download_incorrect(self) -> None:
            """
            Verify that unsupported data formats are handled correctly.
            """
            # TODO(steysie): Check that a correct error is raised with `self.assertRaises`.
            pass


class TestInstantiateExchange(hunitest.TestCase):
    def test_instantiate_exchange(self) -> None:
        """
        Verify that exchange tuple is generated correctly.
        """
        # TODO(steysie): One test to verify types of each of NamedTuple elements is sufficient.
        pass
