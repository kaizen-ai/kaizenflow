import pandas as pd

import dataflow.model.metrics as dtfmodmetr
import helpers.hunit_test as hunitest


class TestConvertToMetricsFormat(hunitest.TestCase):
    def get_data() -> pd.DataFrame:
        data = [
            [0.34, 0.87, 0.986, 0.456, 0.12, 0.65, 0.23],
            [0.44, 0.123, 0.087, 0.5, 0.9, 0.612, 0.023],
            [0.3, 0.876, 0.9, 0.96, 0.1, 0.5, 0.2],
        ]
        columns = pd.MultiIndex.from_tuples(
            [
                ("vwap.ret_0.vol_adj", 101),
                ("vwap.ret_0.vol_adj", 102),
                ("vwap.ret_0.vol_adj", 103),
                ("col2", 101),
                ("col2", 102),
                ("col2", 103),
            ],
            names=["", "asset_id"]
        )
        start_ts = pd.Timestamp("2022-08-28")
        end_ts = pd.Timestamp("2022-09-03")
        idx = pd.date_range(start_ts, end_ts, freq="5T")

    def test1(self) -> None:
