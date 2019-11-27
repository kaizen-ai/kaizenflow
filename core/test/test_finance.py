import logging

import core.finance as fin
import helpers.unit_test as ut
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


class TestComputeRet0FromMultiplePrices1(ut.TestCase):
    def test_usual_case(self):
        # Read multiple futures.
        symbols = tuple("CL NG RB BZ".split())
        nrows = 100
        min_price_dict_df = kut.read_data("D", "continuous", symbols, nrows=nrows)
        # Calculate returns.
        mode = "pct_change"
        col_name = "close"
        actual_result = fin.compute_ret_0_from_multiple_prices(
            min_price_dict_df, col_name, mode
        )
        self.check_string(actual_result.to_string())
