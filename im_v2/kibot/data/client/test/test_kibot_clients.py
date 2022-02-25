from typing import List

import pandas as pd

import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.kibot.data.client.kibot_clients_example as imvkdckcex

# #############################################################################
# TestKibotEquitiesCsvParquetByAssetClient
# #############################################################################


class TestKibotEquitiesCsvParquetByAssetClient(icdctictc.ImClientTestCase):

    def test_read_csv_data2(self) -> None:
        full_symbols = ["kibot::HD", "kibot::AMP"]
        client = imvkdckcex.get_KibotEquitiesCsvParquetByAssetClient_example1(
            False
        )
        #
        expected_length = 262
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::HD", "kibot::AMP"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 08:24:00+00:00, 2015-09-29 11:10:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(262, 6)
                                  full_symbol    open      high       low     close  volume
        timestamp
        2015-09-29 08:24:00+00:00   kibot::HD  102.99  102.99.1  102.99.2  102.99.3   112.0
        2015-09-29 08:25:00+00:00   kibot::HD     NaN       NaN       NaN       NaN     NaN
        2015-09-29 08:26:00+00:00   kibot::HD     NaN       NaN       NaN       NaN     NaN
        ...
        2015-09-29 11:08:00+00:00  kibot::AMP  91.68  91.68  91.65  91.67  1374.0
        2015-09-29 11:09:00+00:00  kibot::AMP  91.62  91.64  91.56  91.56  1494.0
        2015-09-29 11:10:00+00:00  kibot::AMP  91.53  91.57  91.51  91.53  1374.0
        """
        # pylint: enable=line-too-long
        self._test_read_data2(
            client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_csv_data5(self) -> None:
        full_symbols = ["kibot::HD", "kibot::AMP"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        client = imvkdckcex.get_KibotEquitiesCsvParquetByAssetClient_example1(
            False
        )
        #
        expected_length = 19
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::HD", "kibot::AMP"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 09:23:00+00:00, 2015-09-29 09:35:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(19, 6)
                                  full_symbol    open    high     low   close  volume
        timestamp
        2015-09-29 09:23:00+00:00   kibot::HD  102.36  102.36  102.36  102.36   447.0
        2015-09-29 09:24:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        2015-09-29 09:25:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        ...
        2015-09-29 09:34:00+00:00   kibot::HD  102.17  102.33  102.16  102.33   6145.0
        2015-09-29 09:35:00+00:00  kibot::AMP   90.61   90.62    90.5   90.52   2635.0
        2015-09-29 09:35:00+00:00   kibot::HD  102.39  102.49  102.12  102.15  19620.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_csv_unadjusted_data5(self) -> None:
        """
        Note: for current test we use `unadjusted=False` data since the actual unadjusted data is unreachable for now.
        """
        full_symbols = ["kibot::HD", "kibot::AMP"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        client = imvkdckcex.get_KibotEquitiesCsvParquetByAssetClient_example1(True)
        #
        expected_length = 19
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::HD", "kibot::AMP"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 09:23:00+00:00, 2015-09-29 09:35:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(19, 6)
                                  full_symbol    open    high     low   close  volume
        timestamp
        2015-09-29 09:23:00+00:00   kibot::HD  102.36  102.36  102.36  102.36   447.0
        2015-09-29 09:24:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        2015-09-29 09:25:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        ...
        2015-09-29 09:34:00+00:00   kibot::HD  102.17  102.33  102.16  102.33   6145.0
        2015-09-29 09:35:00+00:00  kibot::AMP   90.61   90.62    90.5   90.52   2635.0
        2015-09-29 09:35:00+00:00   kibot::HD  102.39  102.49  102.12  102.15  19620.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_parquet_data5(self) -> None:
        full_symbols = ["kibot::HD", "kibot::QCOM"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        #
        client = imvkdckcex.get_KibotEquitiesCsvParquetByAssetClient_example2(
            False
        )
        #
        expected_length = 24
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::HD", "kibot::QCOM"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 09:23:00+00:00, 2015-09-29 09:35:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(24, 6)
                                  full_symbol    open    high     low   close  volume
        timestamp
        2015-09-29 09:23:00+00:00   kibot::HD  102.36  102.36  102.36  102.36   447.0
        2015-09-29 09:24:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        2015-09-29 09:25:00+00:00   kibot::HD     NaN     NaN     NaN     NaN     NaN
        ...
        2015-09-29 09:34:00+00:00  kibot::QCOM   44.21   44.31   44.21   44.28  21974.0
        2015-09-29 09:35:00+00:00    kibot::HD  102.39  102.49  102.12  102.15  19620.0
        2015-09-29 09:35:00+00:00  kibot::QCOM   44.28   44.34   44.28   44.33  28421.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    @staticmethod
    def _get_expected_column_names() -> List[str]:
        """
        Return a list of expected column names.
        """
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        return expected_column_names


# #############################################################################
# TestKibotEquitiesCsvParquetByAssetClient
# #############################################################################


class TestKibotFuturesCsvParquetByAssetClient(icdctictc.ImClientTestCase):

    def test_read_csv_data2(self) -> None:
        full_symbols = ["kibot::ZI", "kibot::W"]
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example1(
            "continuous"
        )
        #
        expected_length = 3951
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["kibot::ZI", "kibot::W"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-27 19:00:00+00:00, 2009-09-29 21:37:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(3951, 6)
                                  full_symbol   open   high    low  close  volume
        timestamp
        2009-09-27 19:00:00+00:00    kibot::W  462.0  462.1  452.0  457.0   762.0
        2009-09-27 19:01:00+00:00    kibot::W    NaN    NaN    NaN    NaN     NaN
        2009-09-27 19:02:00+00:00    kibot::W    NaN    NaN    NaN    NaN     NaN
        ...
        2009-09-29 21:35:00+00:00   kibot::ZI    NaN    NaN    NaN    NaN     NaN
        2009-09-29 21:36:00+00:00   kibot::ZI    NaN    NaN    NaN    NaN     NaN
        2009-09-29 21:37:00+00:00   kibot::ZI  16.23  16.23  16.23  16.23     1.0
        """
        # pylint: enable=line-too-long
        self._test_read_data2(
            client,
            full_symbols,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_csv_data5(self) -> None:
        full_symbols = ["kibot::ZI", "kibot::W"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example1(
            "continuous"
        )
        #
        expected_length = 19
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {"full_symbol": ["kibot::ZI", "kibot::W"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-29 03:38:00+00:00, 2009-09-29 03:55:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(19, 6)
                                  full_symbol     open    high     low   close  volume
        timestamp
        2009-09-29 03:38:00+00:00    kibot::W  459.250  459.25  459.25  459.25     1.0
        2009-09-29 03:38:00+00:00   kibot::ZI   16.224  16.224  16.204  16.204     4.0
        2009-09-29 03:39:00+00:00   kibot::ZI      NaN     NaN     NaN     NaN     NaN
        ...
        2009-09-29 03:53:00+00:00   kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:54:00+00:00   kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:55:00+00:00   kibot::ZI  16.134  16.134  16.134  16.134     1.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_csv_expiry_data5(self) -> None:
        full_symbols = ["kibot::BB", "kibot::NN"]
        start_ts = pd.Timestamp("2009-09-27T19:45:00+00:00")
        end_ts = pd.Timestamp("2009-09-27T20:05:00+00:00")
        #
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example1(
            "expiry"
        )
        #
        expected_length = 41
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::BB", "kibot::NN"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-27 19:45:00+00:00, 2009-09-27 20:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(41, 6)
                                  full_symbol     open      high      low     close  volume
        timestamp
        2009-09-27 19:45:00+00:00   kibot::BB    139.2    139.24  139.2.1  139.24.1    88.0
        2009-09-27 19:45:00+00:00   kibot::NN  10120.0  10120.10    10110   10120.2  1199.0
        2009-09-27 19:46:00+00:00   kibot::BB      NaN       NaN      NaN       NaN     NaN
        ...
        2009-09-27 20:04:00+00:00   kibot::BB    139.24    139.25  139.24   139.25     1.0
        2009-09-27 20:04:00+00:00   kibot::NN  10085.00  10090.00   10085  10090.0   426.0
        2009-09-27 20:05:00+00:00   kibot::NN  10090.00  10100.00   10085  10085.0  1105.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_parquet_data5(self) -> None:
        full_symbols = ["kibot::ZI", "kibot::EZ"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example2(
            "continuous"
        )
        #
        expected_length = 36
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::ZI", "kibot::EZ"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-29 03:38:00+00:00, 2009-09-29 03:55:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(36, 6)
                                  full_symbol     open     high      low    close  volume
        timestamp
        2009-09-29 03:38:00+00:00   kibot::EZ  108.200  108.200  108.195  108.195   896.0
        2009-09-29 03:38:00+00:00   kibot::ZI   16.224   16.224   16.204   16.204     4.0
        2009-09-29 03:39:00+00:00   kibot::EZ      NaN      NaN      NaN      NaN     NaN
        ...
        2009-09-29 03:54:00+00:00   kibot::ZI      NaN      NaN      NaN      NaN     NaN
        2009-09-29 03:55:00+00:00   kibot::EZ  108.200  108.200  108.195  108.200   190.0
        2009-09-29 03:55:00+00:00   kibot::ZI   16.134   16.134   16.134   16.134     1.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_read_parquet_expiry_data5(self) -> None:
        full_symbols = ["kibot::CL", "kibot::HO"]
        start_ts = pd.Timestamp("2009-09-27T19:28:00+00:00")
        end_ts = pd.Timestamp("2009-09-27T19:50:00+00:00")
        #
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example2(
            "expiry"
        )
        #
        expected_length = 21
        expected_column_names = self._get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["kibot::CL", "kibot::HO"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-27 19:29:00+00:00, 2009-09-27 19:48:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(21, 6)
                                  full_symbol   open   high    low  close  volume
        timestamp
        2009-09-27 19:29:00+00:00   kibot::CL  66.35  66.37  66.35  66.37     5.0
        2009-09-27 19:30:00+00:00   kibot::CL  66.37  66.37  66.36  66.36    12.0
        2009-09-27 19:31:00+00:00   kibot::CL    NaN    NaN    NaN    NaN     NaN
        ...
        2009-09-27 19:47:00+00:00   kibot::CL  66.4900  66.5100  66.4900  66.5100    16.0
        2009-09-27 19:48:00+00:00   kibot::CL  66.4700  66.4700  66.4500  66.4500     3.0
        2009-09-27 19:48:00+00:00   kibot::HO   1.6949   1.6949   1.6949   1.6949     4.0
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_get_metadata1(self) -> None:
        client = imvkdckcex.get_KibotFuturesCsvParquetByAssetClient_example1(
            "continuous"
        )
        expected_length = 14962
        expected_column_names = ["Symbol", "Link", "Description"]
        expected_column_unique_values = None
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[0, 14961]
        columns=Symbol,Link,Description
        shape=(14962, 3)
          Symbol                                                                                                                                                                                                                                                                                                                                                                                             Link                       Description
        0     JY              http://api.kibot.com/?action=download&link=15171e1f1l1cim1k1f1r1e1l1bzmm41ezm1a19im181t1e1t1b191rm41t1c1512161t1r1e1912immpm41rzm1j141l1pimikivm41f1c1e191b1g151pimmjm41r1e151b1e12151e19immjmhmjmhmjmtmpmpm4121f1b19171eimmjm4151e1e15171k1j191c1eimmjm41b191d1t1p151b1r191r1r1f1l1cimmpm41t1r191bim1514191ciz171l1j17151r1em61c191em41a151r1r1v1l1b12im191819mc191b191d1aaiam7v7f  CONTINUOUS JAPANESE YEN CONTRACT
        1  JYF18  http://api.kibot.com/?action=download&link=vrv1vcv9vdvpnmvav9vlvcvdvk4mmhvc4mvbvsnmv2vivcvivkvsvlmhvivpvrvev6vivlvcvsvenmmjmhvl4mvtvhvdvjnmnanunemtmcmhv9vpvcvsvkv5vrvjnmmtmhvlvcvrvkvcvevrvcvsnmmtm3mtm3mtmimjmjmhvev9vkvsv1vcnmmtmhvrvcvcvrv1vavtvsvpvcnmmtmhvkvsv7vivjvrvkvlvsvlvlv9vdvpnmmjmhvivlvsvknmvrvhvsvpn4v1vdvtv1vrvlvcm6vpvsvcmhvbvrvlvlvuvdvkvenmvsv2vsmpvsvkvsv7vbaiama87n7d7n7v         JAPANESE YEN JANUARY 2018
        2  JYF19  http://api.kibot.com/?action=download&link=8r8185898d8pnm8a898l858d8k4mmv854m8b8inm828685868k8i8lmv868p8r8e8j868l858i8enmmgmv8l4m8c8v8d8gnmnansnemcm6mv898p858i8k8t8r8gnmmcmv8l858r8k858e8r858inmmcm3mcm3mcm6mgmgmv8e898k8i8185nmmcmv8r85858r818a8c8i8p85nmmcmv8k8i8u868g8r8k8l8i8l8l898d8pnmmgmv868l8i8knm8r8v8i8pn4818d8c818r8l85mj8p8i85mv8b8r8l8l8s8d8k8enm8i828imp8i8k8i8u8baiama87n7u7n7v         JAPANESE YEN JANUARY 2019
        ...
        14959  HGTZ16  http://api.kibot.com/?action=download&link=lcltl7lvl3ljr1l5lvldl7l3lmg116l7g1l8l9r1l4lfl7lflml9ld16lfljlclulalfldl7l9lur11k16ldg1lpl6l3lkr1r4r9rmls1p1m16lvljl7l9lml2lclkr11p16ldl7lclml7lulcl7l9r11p1i1p1i1p1f1k1k16lulvlml9ltl7r11p16lcl7l7lcltl5lpl9ljl7r11p16lml9lzlflklclmldl9ldldlvl3ljr11k16lfldl9lmr1lcl6l9ljrgltl3lpltlcldl71aljl9l716l8lcldldlnl3lmlur1l9l4l91jl9lml9lzl8alapac1z7n7c7n72  COPPER (TAS) DECEMBER 2016
        14960  HGTZ17  http://api.kibot.com/?action=download&link=lclkl7lvl3lj51l2lvldl7l3lmg116l7g1lnl951lalrl7lrlml9ld16lrljlclflulrldl7l9lf511416ldg1lpl6l3l4515a595mle1p1d16lvljl7l9lmlslcl4511p16ldl7lclml7lflcl7l9511p1i1p1i1p1r141416lflvlml9lkl7511p16lcl7l7lclkl2lpl9ljl7511p16lml9lzlrl4lclmldl9ldldlvl3lj511416lrldl9lm51lcl6l9lj5glkl3lplklcldl71uljl9l716lnlcldldltl3lmlf51l9lal91jl9lml9lzlnalapac1z7n7r7n72  COPPER (TAS) DECEMBER 2017
        14961  HGTZ18  http://api.kibot.com/?action=download&link=lpltl7lvl3l6r1l5lvldl7l3lmg112l7g1lcljr1lalfl7lflmljld12lfl6lpl9lulfldl7ljl9r11412ldg1lkl2l3l4r1rarjrmle1k1712lvl6l7ljlmlslpl4r11k12ldl7lplml7l9lpl7ljr11k1i1k1i1k1f141412l9lvlmljltl7r11k12lpl7l7lpltl5lkljl6l7r11k12lmljlzlfl4lplmldljldldlvl3l6r11412lfldljlmr1lpl2ljl6rgltl3lkltlpldl71ul6ljl712lclpldldlnl3lml9r1ljlalj16ljlmljlzlcalapac1z7n7d7n72  COPPER (TAS) DECEMBER 2018
        """
        # pylint: enable=line-too-long
        self._test_get_metadata1(
            client,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    @staticmethod
    def _get_expected_column_names() -> List[str]:
        """
        Return a list of expected column names.
        """
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        return expected_column_names
