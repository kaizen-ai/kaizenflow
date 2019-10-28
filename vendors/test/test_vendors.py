import logging
import os

import bs4
import pytest

import helpers.dbg as dbg
import helpers.s3 as hs3
import helpers.system_interaction as si
import helpers.unit_test as ut
import vendors.cme.read as cmer
import vendors.etfs.utils as etfut
# TODO: https://github.com/ParticleDev/commodity_research/issues/456
# import vendors.eurostat.base_classes as euro_bc
# import vendors.eurostat.filler_versions as euro_fv
import vendors.first_rate.read as frr
import vendors.kibot.utils as kut
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
        self.check_string(txt, fuzzy_match=True)


# #############################################################################
# kibot/utils.py
# #############################################################################


class Test_kibot_utils1(ut.TestCase):
    def test_read_data1(self):
        # TODO(gp): Use unit test cache.
        file_name = os.path.join(
            hs3.get_path(), "kibot/All_Futures_Contracts_1min/ES.csv.gz"
        )
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


# #############################################################################
# pandas_datareader/utils.py
# #############################################################################


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


@pytest.mark.slow()
class Test_first_rate1(ut.TestCase):
    def test_downloader1(self):
        tmp_dir = self.get_scratch_space()
        cmd = []
        # TODO(Julia): Rename download.py
        cmd.append("vendors/first_rate/download.py")
        cmd.append("--zipped_dst_dir %s/zipped" % tmp_dir)
        cmd.append("--unzipped_dst_dir %s/unzipped" % tmp_dir)
        cmd.append("--pq_dst_dir %s/pq" % tmp_dir)
        cmd.append("--max_num_files 1")
        cmd = " ".join(cmd)
        si.system(cmd)
        # TODO(Julia): Test the dowloaded data with the code below.
        pq_dir = "%s/pq" % tmp_dir
        file_name = os.listdir(pq_dir)[0]
        file_path = os.path.join(pq_dir, file_name)
        frr.read_data(file_path)

    def test_reader1(self):
        # TODO(Julia): We want to add a test the official s3 location of this
        # data. The data has been uploaded.
        pass


# #############################################################################
# cme
# #############################################################################


@pytest.mark.slow()
class Test_cme1(ut.TestCase):
    def test_downloader1(self):
        tmp_dir = self.get_scratch_space()
        cmd = []
        # TODO(Julia): Rename download.py
        cmd.append("vendors/cme/download.py")
        cmd.append(
            "--download_url"
            " https://www.cmegroup.com/CmeWS/mvc/ProductSlate/V1/Download.xls"
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


# #############################################################################
# Eurostat
# #############################################################################

# TODO: https://github.com/ParticleDev/commodity_research/issues/456

# @pytest.mark.slow()
# class Test_eurostat(ut.TestCase):
#     @staticmethod
#     def _get_class(filler_version):
#         version_config = euro_fv.EUROSTAT_FILLERS[filler_version]
#         return version_config['class']
#
#     @classmethod
#     def _get_instance(cls, filler_version):
#         version_class = cls._get_class(filler_version=filler_version)
#         return version_class(filler_version=filler_version)
#
#     # General cases, no depends on optional settings.
#     # These cases can be moved in upper level.
#     def test_filler_versions(self):
#         for version in euro_fv.EUROSTAT_FILLERS:
#             self._get_instance(filler_version=version)
#
#     # Tests for interface EurostatFileFillerV1
#
#     def test_EurostatFileFillerV1_source_availability(self):
#         for filler_version in euro_fv.EUROSTAT_FILLERS:
#             version_class = self._get_instance(filler_version=filler_version)
#             if issubclass(version_class, euro_bc.EurostatFileFillerV1):
#                 version_instance = version_class(filler_version=filler_version)
#                 source = version_instance.get_source_page()
#                 dbg.dassert_isinstance(source, bs4.BeautifulSoup)
#
#     def test_EurostatFileFillerV1_get_links(self):
#         for filler_version in euro_fv.EUROSTAT_FILLERS:
#             version_class = self._get_instance(filler_version=filler_version)
#             if issubclass(version_class, euro_bc.EurostatFileFillerV1):
#                 version_instance = version_class(filler_version=filler_version)
#                 links = version_instance.get_links()
#                 dbg.dassert_isinstance(links, list)
#                 dbg.dassert_lte(0,
#                                 len(links),
#                                 msg='No links found. Page structure changed.')
#
#     def test_data_reader(self):
#         for version in euro_fv.EUROSTAT_FILLERS:
#             version_class = self._get_instance(filler_version=version)
#             version_class.data_reader().__next__()
