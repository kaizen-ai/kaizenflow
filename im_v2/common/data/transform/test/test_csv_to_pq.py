import os

import pytest
import pandas as pd

import helpers.io_ as hio
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest


class TestCsvToPq(hunitest.TestCase):
    def test1(self) -> None:
        test_dir = self.get_scratch_space()
        pq_dir_path = os.path.join(test_dir, "pq_dir")
        csv_dir_path = os.path.join(test_dir, "csv_dir")
        hio.create_dir(csv_dir_path, False)
        print(csv_dir_path)
        self._generate_csv_files(csv_dir_path)
        cmd = (
            "im_v2/common/data/transform/csv_to_pq.py"
            f" --src_dir {csv_dir_path}"
            f" --dst_dir {pq_dir_path}"
        )
        hsysinte.system(cmd)
        df = pd.read_parquet(os.path.join(pq_dir_path, "test.pq"))
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    def _generate_csv_files(self, path: str) -> None:
        d = {        
            'timestamp':[1638646800000,1638646860000],
            'open':[49317.68,49330.63],
            'high':[49346.95,49400.98],
            'volume':[23.13681,61.99752],
            'low':[49315.45,49322.78],
            'close':[49330.63,49325.23],
            'currency_pair':["BTC_USDT","BTC_USDT"],
            'created_at':["2021-12-07 13:01:20.183463+00:00",
            "2021-12-07 13:01:20.183463+00:00"],
            'exchange_id':["binance","binance"]
        }
        df = pd.DataFrame(data=d)
        df.to_csv(os.path.join(path, "test.csv"))
        return df
