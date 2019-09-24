import logging

import helpers.unit_test as ut
import vendors.etfs.utils as etfut
import vendors.kibot.utils as kut

# #############################################################################
# pandas_datareader/utils.py
# #############################################################################
import vendors.pandas_datareader.utils as pdut

_LOG = logging.getLogger(__name__)


# #############################################################################
# etfs/utils.py
# #############################################################################


class Test_etfs_utils1(ut.TestCase):
    def test_MasterdataReports1(self):
        mrep = etfut.MasterdataReports()
        meta_df = mrep.get_metadata()
        #
        txt = []
        txt.append("meta_df.shape=%s" % str(meta_df.shape))
        #
        meta_df_as_str = meta_df.iloc[:, 0]
        txt.append("meta_df=%s" % meta_df_as_str)
        #
        meta_df_as_str = meta_df.iloc[0, :]
        txt.append("meta_df=%s" % meta_df_as_str)
        txt = "\n".join(txt)
        _LOG.debug("txt=%s", txt)
        #
        self.check_string(txt)


# #############################################################################
# kibot/utils.py
# #############################################################################


class Test_kibot_utils1(ut.TestCase):
    def test_read_data1(self):
        # TODO(gp): Use unit test cache.
        file_name = "s3://alphamatic/kibot/All_Futures_Contracts_1min/ES.csv.gz"
        nrows = 100
        df = kut._read_data(file_name, nrows)
        _LOG.debug("df=%s", df.head())
        #
        df2 = kut._read_data_from_disk_cache(file_name, nrows)
        _LOG.debug("df2=%s", df2.head())
        self.assertTrue(df.equals(df2))
        #
        df3 = kut.read_data(file_name, nrows)
        _LOG.debug("df3=%s", df3.head())
        self.assertTrue(df.equals(df3))
        #
        self.check_string(ut.get_df_signature(df))

    def _helper_read_metadata(self, func):
        df = func()
        self.check_string(ut.get_df_signature(df))

    def test_read_metadata1(self):
        self._helper_read_metadata(kut.read_metadata1)

    def test_read_metadata2(self):
        self._helper_read_metadata(kut.read_metadata2)

    def test_read_metadata3(self):
        self._helper_read_metadata(kut.read_metadata3)

    def test_read_metadata4(self):
        self._helper_read_metadata(kut.read_metadata4)


class Test_pandas_datareader_utils1(ut.TestCase):
    def test_get_multiple_data1(self):
        ydq = pdut.YahooDailyQuotes()
        tickers = "SPY IVV".split()
        df = ydq.get_multiple_data("Adj Close", tickers)
        #
        self.check_string(ut.get_df_signature(df))
