import im_v2.common.universe.test.test_universe as imvcountt


class TestGetUniverseFilePath1(imvcountt.TestGetUniverseFilePath1_TestCase):
    def test_get_universe_file_path(self) -> None:
        """
        A smoke test to test correct file path return when correct version is
        provided.
        """
        self._test_get_universe_file_path("example1", "v1")

    def test_get_latest_file_version(self) -> None:
        """
        Verify that the max universe version is correctly detected and
        returned.
        """
        self._test_get_latest_file_version("example1")


class TestGetUniverse1(imvcountt.TestGetUniverse1_TestCase):
    def test_get_universe_example1(self) -> None:
        """
        A smoke test to verify that example universe loads correctly.
        """
        self._test_get_universe1("example1")
