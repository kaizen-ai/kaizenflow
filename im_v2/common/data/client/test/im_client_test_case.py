"""
Import as:

import im_v2.common.data.client.test.im_client_test_case as icdctictc
"""

from typing import Any, List

import pandas as pd

import helpers.hunit_test as hunitest
import im_v2.common.data.client as icdc

# #############################################################################
# ImClientTestCase
# #############################################################################


class ImClientTestCase(hunitest.TestCase):
    """
    Help test classes for classes derived from `ImClient` by implementing
    template test methods for the interface methods in any `ImClient`.
    """

    # TODO(gp): To enforce that all methods are called we could add corresponding
    #  abstract methods to the test methods.

    def _test_read_data1(
        self,
        im_client: icdc.ImClient,
        full_symbol: icdc.FullSymbol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for one full symbol
        - start_ts = end_ts = None
        - resample_1min = True
        """
        full_symbols = [full_symbol]
        im_client.resample_1min = True
        start_ts = None
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    def _test_read_data2(
        self,
        im_client: icdc.ImClient,
        full_symbols: List[icdc.FullSymbol],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for two or more full symbols
        - start_ts = end_ts = None
        - resample_1min = True
        """
        start_ts = None
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    def _test_read_data3(
        self,
        im_client: icdc.ImClient,
        full_symbols: List[icdc.FullSymbol],
        start_ts: pd.Timestamp,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for two or more symbols
        - specified start_ts
        - end_ts = None
        - resample_1min = True
        """
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    def _test_read_data4(
        self,
        im_client: icdc.ImClient,
        full_symbols: List[icdc.FullSymbol],
        end_ts: pd.Timestamp,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for two or more symbols
        - start_ts = None
        - specified end_ts
        - resample_1min = True
        """
        start_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    def _test_read_data5(
        self,
        im_client: icdc.ImClient,
        full_symbols: List[icdc.FullSymbol],
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for two or more symbols
        - specified start_ts and end_ts
        - resample_1min = True
        """
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    def _test_read_data6(
        self, im_client: icdc.ImClient, full_symbol: icdc.FullSymbol
    ) -> None:
        """
        Test:
        - error is raised when an unsupported full symbol is provided
        - start_ts = end_ts = None
        - resample_1min = True
        """
        full_symbols = [full_symbol]
        start_ts = None
        end_ts = None
        # TODO(gp): We should raise a more specific assertion and / or
        #  check part of the exception as a string.
        with self.assertRaises(AssertionError):
            im_client.read_data(full_symbols, start_ts, end_ts)

    def _test_read_data7(
        self,
        im_client: icdc.ImClient,
        full_symbols: List[icdc.FullSymbol],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Test:
        - reading data for two or more full symbols
        - reading not resampled data
        - start_ts = end_ts = None
        - resample_1min = False
        """
        start_ts = None
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        self.check_df_output(actual_df, *args, **kwargs)

    # ////////////////////////////////////////////////////////////////////////

    def _test_get_start_ts_for_symbol1(
        self,
        im_client: icdc.ImClient,
        full_symbol: icdc.FullSymbol,
        expected_start_ts: pd.Timestamp,
    ) -> None:
        """
        Test that the earliest timestamp available is computed correctly.
        """
        actual_start_ts = im_client.get_start_ts_for_symbol(full_symbol)
        self.assertEqual(actual_start_ts, expected_start_ts)

    def _test_get_end_ts_for_symbol1(
        self,
        im_client: icdc.ImClient,
        full_symbol: icdc.FullSymbol,
        expected_end_ts: pd.Timestamp,
    ) -> None:
        """
        Test that the latest timestamp available is computed correctly.
        """
        actual_end_ts = im_client.get_end_ts_for_symbol(full_symbol)
        # TODO(Grisha): use `assertGreater` when start downloading more data.
        self.assertEqual(actual_end_ts, expected_end_ts)

    # ////////////////////////////////////////////////////////////////////////

    def _test_get_universe1(
        self,
        # TODO(Grisha): pass vendor when we start testing `CDD`.
        im_client: icdc.ImClient,
        expected_length: int,
        expected_first_elements: List[icdc.FullSymbol],
        expected_last_elements: List[icdc.FullSymbol],
    ) -> None:
        """
        Test that universe is computed correctly.
        """
        # TODO(gp): We might want to sort actual and expected universe for
        #  stability.
        universe = im_client.get_universe()
        actual_length = len(universe)
        actual_first_elements = universe[:3]
        actual_last_elements = universe[-3:]
        self.assertEqual(actual_length, expected_length)
        self.assertEqual(actual_first_elements, expected_first_elements)
        self.assertEqual(actual_last_elements, expected_last_elements)

    # TODO(Grisha): @Dan move to `ImClientMetadataTestCase`.
    def _test_get_metadata1(
        self, im_client: icdc.ImClient, *args: Any, **kwargs: Any
    ) -> None:
        """
        Test that metadata is computed correctly.
        """
        actual_df = im_client.get_metadata()
        self.check_df_output(actual_df, *args, **kwargs)
