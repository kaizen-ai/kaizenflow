import pandas as pd

import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.kibot.data.client.test.kibot_clients_example as ikidctkce

# #############################################################################
# TestKibotEquitiesCsvParquetByAssetClient
# #############################################################################


class TestKibotEquitiesCsvParquetByAssetClient(icdctictc.ImClientTestCase):

    def test_read_data2(self) -> None:
        full_symbols = ["kibot::HD"]
        client = ikidctkce.get_KibotEquitiesCsvParquetByAssetClient_example1(
            False
        )
        #
        expected_length = 161
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::HD"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 08:24:00+00:00, 2015-09-29 11:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(161, 6)
                                  full_symbol    open      high       low     close  volume
        timestamp
        2015-09-29 08:24:00+00:00   kibot::HD  102.99  102.99.1  102.99.2  102.99.3   112.0
        2015-09-29 08:25:00+00:00   kibot::HD     NaN       NaN       NaN       NaN     NaN
        2015-09-29 08:26:00+00:00   kibot::HD     NaN       NaN       NaN       NaN     NaN
        ...
        2015-09-29 11:02:00+00:00   kibot::HD  102.03  102.03  101.95  101.99   6028.0
        2015-09-29 11:03:00+00:00   kibot::HD  101.99  102.08  101.99  102.06  13641.0
        2015-09-29 11:04:00+00:00   kibot::HD  102.06  102.17  102.03  102.17  35040.0
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
        full_symbols = ["kibot::HD"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        client = ikidctkce.get_KibotEquitiesCsvParquetByAssetClient_example1(
            False
        )
        #
        expected_length = 13
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::HD"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 09:23:00+00:00, 2015-09-29 09:35:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(13, 6)
                                   full_symbol    open    high     low   close  volume
        timestamp
        2015-09-29 09:23:00+00:00  kibot::HD  102.36  102.36  102.36  102.36   447.0
        2015-09-29 09:24:00+00:00  kibot::HD     NaN     NaN     NaN     NaN     NaN
        2015-09-29 09:25:00+00:00  kibot::HD     NaN     NaN     NaN     NaN     NaN
        ...
        2015-09-29 09:33:00+00:00  kibot::HD  102.17  102.21  102.08  102.17  15277.0
        2015-09-29 09:34:00+00:00  kibot::HD  102.17  102.33  102.16  102.33   6145.0
        2015-09-29 09:35:00+00:00  kibot::HD  102.39  102.49  102.12  102.15  19620.0
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
        full_symbols = ["kibot::HD"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        #
        client = ikidctkce.get_KibotEquitiesCsvParquetByAssetClient_example2(
            False
        )
        #
        expected_length = 13
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::HD"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2015-09-29 09:23:00+00:00, 2015-09-29 09:35:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(13, 6)
                                   full_symbol    open    high     low   close  volume
        timestamp
        2015-09-29 09:23:00+00:00  kibot::HD  102.36  102.36  102.36  102.36   447.0
        2015-09-29 09:24:00+00:00  kibot::HD     NaN     NaN     NaN     NaN     NaN
        2015-09-29 09:25:00+00:00  kibot::HD     NaN     NaN     NaN     NaN     NaN
        ...
        2015-09-29 09:33:00+00:00  kibot::HD  102.17  102.21  102.08  102.17  15277.0
        2015-09-29 09:34:00+00:00  kibot::HD  102.17  102.33  102.16  102.33   6145.0
        2015-09-29 09:35:00+00:00  kibot::HD  102.39  102.49  102.12  102.15  19620.0
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


# #############################################################################
# TestKibotEquitiesCsvParquetByAssetClient
# #############################################################################


class TestKibotFuturesCsvParquetByAssetClient(icdctictc.ImClientTestCase):

    def test_read_data2(self) -> None:
        full_symbols = ["kibot::ZI"]
        client = ikidctkce.get_KibotFuturesCsvParquetByAssetClient_example1(
            "continuous"
        )
        #
        expected_length = 1573
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::ZI"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-28 19:25:00+00:00, 2009-09-29 21:37:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(1573, 6)
                                  full_symbol   open     high      low    close  volume
        timestamp
        2009-09-28 19:25:00+00:00   kibot::ZI  16.23  16.23.1  16.23.2  16.23.3     1.0
        2009-09-28 19:26:00+00:00   kibot::ZI    NaN      NaN      NaN      NaN     NaN
        2009-09-28 19:27:00+00:00   kibot::ZI    NaN      NaN      NaN      NaN     NaN
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
        full_symbols = ["kibot::ZI"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = ikidctkce.get_KibotFuturesCsvParquetByAssetClient_example1(
            "continuous"
        )
        #
        expected_length = 18
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::ZI"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-29 03:38:00+00:00, 2009-09-29 03:55:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(18, 6)
                                   full_symbol    open    high     low   close  volume
        timestamp
        2009-09-29 03:38:00+00:00  kibot::ZI  16.224  16.224  16.204  16.204     4.0
        2009-09-29 03:39:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:40:00+00:00  kibot::ZI  16.210   16.21   16.21   16.21     1.0
        ...
        2009-09-29 03:53:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:54:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:55:00+00:00  kibot::ZI  16.134  16.134  16.134  16.134     1.0
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
        full_symbols = ["kibot::BB"]
        start_ts = pd.Timestamp("2009-09-27T19:45:00+00:00")
        end_ts = pd.Timestamp("2009-09-27T20:05:00+00:00")
        #
        client = ikidctkce.get_KibotFuturesCsvParquetByAssetClient_example1(
            "expiry"
        )
        #
        expected_length = 20
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::BB"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-27 19:45:00+00:00, 2009-09-27 20:04:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(20, 6)
                                  full_symbol   open    high      low     close  volume
        timestamp
        2009-09-27 19:45:00+00:00   kibot::BB  139.2  139.24  139.2.1  139.24.1    88.0
        2009-09-27 19:46:00+00:00   kibot::BB    NaN     NaN      NaN       NaN     NaN
        2009-09-27 19:47:00+00:00   kibot::BB    NaN     NaN      NaN       NaN     NaN
        ...
        2009-09-27 20:02:00+00:00   kibot::BB     NaN     NaN     NaN     NaN     NaN
        2009-09-27 20:03:00+00:00   kibot::BB  139.27  139.27  139.26  139.26    16.0
        2009-09-27 20:04:00+00:00   kibot::BB  139.24  139.25  139.24  139.25     1.0
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
        full_symbols = ["kibot::ZI"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = ikidctkce.get_KibotFuturesCsvParquetByAssetClient_example2(
            "continuous"
        )
        #
        expected_length = 18
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::ZI"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2009-09-29 03:38:00+00:00, 2009-09-29 03:55:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(18, 6)
                                   full_symbol    open    high     low   close  volume
        timestamp
        2009-09-29 03:38:00+00:00  kibot::ZI  16.224  16.224  16.204  16.204     4.0
        2009-09-29 03:39:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:40:00+00:00  kibot::ZI  16.210  16.210  16.210  16.210     1.0
        ...
        2009-09-29 03:53:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:54:00+00:00  kibot::ZI     NaN     NaN     NaN     NaN     NaN
        2009-09-29 03:55:00+00:00  kibot::ZI  16.134  16.134  16.134  16.134     1.0
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

    def test_read_parquet_expiry_data6(self) -> None:
        full_symbols = ["kibot::CT"]
        start_ts = pd.Timestamp("2007-05-16T09:59:00+00:00")
        end_ts = pd.Timestamp("2007-05-16T10:19:00+00:00")
        #
        client = ikidctkce.get_KibotFuturesCsvParquetByAssetClient_example2(
            "expiry"
        )
        #
        expected_length = 21
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        expected_column_unique_values = {"full_symbol": ["kibot::CT"]}
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2007-05-16 09:59:00+00:00, 2007-05-16 10:19:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(21, 6)
                                  full_symbol   open   high    low  close  volume
        timestamp
        2007-05-16 09:59:00+00:00   kibot::CT  48.20  48.24  48.20  48.24     6.0
        2007-05-16 10:00:00+00:00   kibot::CT  48.25  48.25  48.24  48.24    27.0
        2007-05-16 10:01:00+00:00   kibot::CT  48.20  48.20  48.20  48.20    11.0
        ...
        2007-05-16 10:17:00+00:00   kibot::CT  48.25  48.32  48.25  48.32     7.0
        2007-05-16 10:18:00+00:00   kibot::CT  48.30  48.31  48.30  48.31     9.0
        2007-05-16 10:19:00+00:00   kibot::CT  48.30  48.30  48.30  48.30     1.0
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