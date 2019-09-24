import logging

import pytest

import helpers.unit_test as ut
import vendors.etfs.utils as etfut
import vendors.kibot.utils as kut

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
        #
        df2 = kut._read_data_from_disk_cache(file_name, nrows)
        self.assertTrue(df.equals(df2))
        #
        df3 = kut.read_data(file_name, nrows)
        self.assertTrue(df.equals(df3))
        #
        self.check_string(str(df.head(5)))

    @pytest.mark.skip
    def test_read_metadata1(self):
        df = kut.read_metadata1()
        self.check_string(str(df.head(5)))

    @pytest.mark.skip
    def test_read_metadata2(self):
        df = kut.read_metadata2()
        self.check_string(str(df.head(5)))
