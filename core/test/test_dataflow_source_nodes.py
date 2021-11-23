import pytest

import core.dataflow_source_nodes as cdtfsonod
import helpers.unit_test as hunitest


class TestKibotEquityReader(hunitest.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        node = cdtfsonod.KibotEquityReader(
            "reader",
            ["AAPL", "GOOG"],
            "T",
            "2018-01-02 09:30:00",
            "2018-01-02 10:00:00",
        )
        df = node.fit()["df_out"]
        df_str = hunitest.convert_df_to_string(df, index=True)
        self.check_string(df_str)
