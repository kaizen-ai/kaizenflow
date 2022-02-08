"""
Import as:

import im_v2.common.data.client.test.im_client_test_case as icdctictc
"""

from typing import Any, List, Optional

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.client as icdc


# TODO(gp): @Grisha This is a common pattern to check a df, that we want to
#  generalize and move to `unit_test.py`
# def _check_df_output(
#    self_: Any,
#    actual_df: pd.DataFrame,
#    expected_length: Optional[int],
#    expected_column_names: Optional[List[str]],
#    expected_column_values: Optional[Dict[str, List[str]],
#    expected_signature: str,
# """
# Verify that actual outcome dataframe matches the expected one.
#
# :param actual_df: actual outcome dataframe
# :param expected_length: expected outcome dataframe length
#    - If `None` skip the check
# :param expected_column_names: expected columns as a set
#    - If `None` skip the check
# :param expected_column_values: dict of column names and values that they should
#      contain. If `None` skip the check
# :param expected_signature: expected outcome as string
# """
def _check_output(
    self_: Any,
    actual_df: pd.DataFrame,
    expected_length: int,
    # TODO(gp): @Grisha exchange_ids and currency_pairs are specific of a type of
    #  assets. Classes should return `full_symbol = exchange_ids::assert_id`.
    expected_exchange_ids: Optional[List[str]],
    expected_currency_pairs: Optional[List[str]],
    expected_signature: str,
) -> None:
    """
    Verify that actual outcome dataframe matches the expected one.

    :param actual_df: actual outcome dataframe
    :param expected_length: expected outcome dataframe length
    :param expected_exchange_ids: list of expected exchange ids
    :param expected_currency_pairs: list of expected currency pairs
    :param expected_signature: expected outcome as string
    """
    # Build signature.
    act = []
    #
    actual_df = actual_df[sorted(actual_df.columns)]
    act.append(hpandas.df_to_str(actual_df, print_shape_info=True, tag="df"))
    #
    if expected_exchange_ids is not None:
        actual_exchange_ids = sorted(
            list(actual_df["exchange_id"].dropna().unique())
        )
        act.append("exchange_ids=%s" % ",".join(actual_exchange_ids))
    #
    if expected_currency_pairs is not None:
        actual_currency_pairs = sorted(
            list(actual_df["currency_pair"].dropna().unique())
        )
        act.append("currency_pairs=%s" % ",".join(actual_currency_pairs))
    #
    actual_signature = "\n".join(act)
    # Check.
    self_.assert_equal(
        actual_signature,
        expected_signature,
        dedent=True,
        fuzzy_match=True,
    )
    # Check output df length.
    self_.assert_equal(str(expected_length), str(actual_df.shape[0]))
    if expected_exchange_ids is not None:
        # Check unique exchange ids in the output df.
        self_.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
    if expected_currency_pairs is not None:
        # Check unique currency pairs in the output df.
        self_.assert_equal(
            str(actual_currency_pairs), str(expected_currency_pairs)
        )


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
        """
        full_symbols = [full_symbol]
        start_ts = None
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(self, actual_df, *args, **kwargs)

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
        """
        start_ts = None
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(self, actual_df, *args, **kwargs)

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
        """
        end_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(self, actual_df, *args, **kwargs)

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
        """
        start_ts = None
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(self, actual_df, *args, **kwargs)

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
        """
        actual_df = im_client.read_data(full_symbols, start_ts, end_ts)
        _check_output(self, actual_df, *args, **kwargs)

    def _test_read_data6(
        self, im_client: icdc.ImClient, full_symbol: icdc.FullSymbol
    ) -> None:
        """
        Test:
        - error is raised when an unsupported full symbol is provided
        - start_ts = end_ts = None
        """
        full_symbols = [full_symbol]
        start_ts = None
        end_ts = None
        # TODO(gp): We should raise a more specific assertion and / or
        #  check part of the exception as a string.
        with self.assertRaises(AssertionError):
            im_client.read_data(full_symbols, start_ts, end_ts)

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
