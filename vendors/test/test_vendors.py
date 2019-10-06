import logging

import pytest

import helpers.system_interaction as si
import helpers.unit_test as ut
import vendors.cme.reader as cmer
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


# TODO(gp): #354
@pytest.mark.skipif('si.get_user_name() == "jenkins"')
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
        self.check_string(txt, fuzzy_match=True)


# #############################################################################
# kibot/utils.py
# #############################################################################


# TODO(gp): #354
@pytest.mark.skipif('si.get_user_name() == "jenkins"')
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
        self.check_string(ut.get_df_signature(df), fuzzy_match=True)

    def test_read_metadata1(self):
        self._helper_read_metadata(kut.read_metadata1)

    def test_read_metadata2(self):
        self._helper_read_metadata(kut.read_metadata2)

    def test_read_metadata3(self):
        self._helper_read_metadata(kut.read_metadata3)

    def test_read_metadata4(self):
        self._helper_read_metadata(kut.read_metadata4)


# TODO(gp): #354
@pytest.mark.skipif('si.get_user_name() == "jenkins"')
class Test_pandas_datareader_utils1(ut.TestCase):
    def test_get_multiple_data1(self):
        ydq = pdut.YahooDailyQuotes()
        tickers = "SPY IVV".split()
        df = ydq.get_multiple_data("Adj Close", tickers)
        #
        self.check_string(ut.get_df_signature(df))


# #############################################################################
# first_rate
# #############################################################################


@pytest.mark.skip
@pytest.mark.slow()
class Test_first_rate1(ut.TestCase):
    def test_downloader1(self):
        tmp_dir = self.get_scratch_space()
        cmd = []
        cmd.append("vendors/first_rate/utils.py")
        cmd.append("--zipped_dst_dir %s/zipped" % tmp_dir)
        cmd.append("--unzipped_dst_dir %s/unzipped" % tmp_dir)
        cmd.append("--pq_dst_dir %s/pq" % tmp_dir)
        cmd.append("--max_num_files 1")
        cmd = " ".join(cmd)
        si.system(cmd)

    # TODO(Julia): Add test for reader.

# #############################################################################
# cme
# #############################################################################

@pytest.mark.skip
@pytest.mark.slow()
class Test_cme1(ut.TestCase):
    def test_downloader1(self):
        tmp_dir = self.get_scratch_space()
        cmd = []
        cmd.append("vendors/cme/utils.py")
        cmd.append(
            "--download_url https://www.cmegroup.com/CmeWS/mvc/ProductSlate/V1/Download.xls"
        )
        cmd.append("--product_list %s/product_list.xls" % tmp_dir)
        cmd.append("--product_specs %s/list_with_specs.csv" % tmp_dir)
        cmd.append("--max_num_specs 1")
        cmd = " ".join(cmd)
        si.system(cmd)

    def test_read_product_specs(self):
        cmer.read_product_specs()

    def test_read_product_list(self):
        cmer.read_product_list()
