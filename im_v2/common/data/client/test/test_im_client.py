import helpers.hunit_test as hunitest



class TestImClient(hunitest.TestCase, abc.ABC):


    @abstractmethod
    def test_read_data1(self, im_client, full_symbol, exp) -> None:
        """
        Test:
        - reading data for one symbol
        - start_ts = end_ts = None
        """
        act = self.im_client.read_data(*args, **kwargs)
        # We should use _check_output instead of assert_equal
        self.assert_equal(act, exp)

    @abstractmethod
    def test_read_data2(self, im_client, full_symbols, exp) -> None:
        """
        Test:
        - reading data for two symbols
        - start_ts = end_ts = None
        """

    @abstractmethod
    def test_read_data3(self, im_client, full_symbols, start_ts, exp) -> None:
        """
        Test:
        - reading data for two symbols
        - specified start_ts
        - end_ts = None
        """

    def test_read_data4(self) -> None:
        """
        Test:
        - reading data for two symbols
        - start_ts = None
        - specified end_ts
        """

    def test_read_data5(self) -> None:
        """
        Test:
        - reading data for two symbols
        - specified start_ts and end_ts
        """

    def test_get_start_ts_for_symbol1(self) -> None:
        """

        """

    def test_get_end_ts_for_symbol1(self) -> None:
        """

        """

    def test_get_universe1(self) -> None:
        """

        """


class TestCcxtCsvPqByAssetClient1(TestImClient):

    def test_read_data1(self):
        """
        Called for each test method.
        """
        im_client = CcxtCsvPqByAssetClient(...)
        full_symbols = ['']
        exp = """
        """
        super().test_read_data1(im_client, full_symbol, exp)

    def test_read_data2(self):
        """
        Called for each test method.
        """
        im_client = CcxtCsvPqByAssetClient(...)
        full_symbols = ['', '']
        exp = """
        """
        self._test_read_data2(im_client, full_symbol, exp)
