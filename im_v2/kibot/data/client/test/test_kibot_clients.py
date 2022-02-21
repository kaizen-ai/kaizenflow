import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.kibot.data.client.kibot_clients as imvkdckicl
import im_v2.kibot.data.client.test.kibot_clients_example as ikidctkce


# TODO(Dan): @Max CmTask1245.
class TestKibotEquitiesCsvParquetByAssetClient(icdctictc.ImClientTestCase):
    def test_read_csv_data5(self) -> None:
        full_symbols = ["kibot::HD"]
        start_ts = pd.Timestamp("2015-09-29T09:23:00+00:00")
        end_ts = pd.Timestamp("2015-09-29T09:35:00+00:00")
        client = ikidctkce.get_TestKibotEquitiesCsvParquetByAssetClient_example1()
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
        client = ikidctkce.get_TestKibotEquitiesCsvParquetByAssetClient_example2()
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


class TestKibotFuturesCsvParquetByAssetClient(icdctictc.ImClientTestCase):
    def test_read_csv_data5(self) -> None:
        full_symbols = ["kibot::ZI"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = ikidctkce.get_TestKibotFuturesCsvParquetByAssetClient_example1()
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

    def test_read_parquet_data5(self) -> None:
        full_symbols = ["kibot::ZI"]
        start_ts = pd.Timestamp("2009-09-29T03:38:00+00:00")
        end_ts = pd.Timestamp("2009-09-29T03:55:00+00:00")
        #
        client = ikidctkce.get_TestKibotFuturesCsvParquetByAssetClient_example2()
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
