import os

import pytest
import pandas as pd
from typing import Tuple

import helpers.io_ as hio
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest


class TestCsvToPq(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that generated .pq file is correct.
        """
        csv_dir_path, pq_dir_path = self._generate_files()
        cmd = (
            "im_v2/common/data/transform/csv_to_pq.py"
            f" --src_dir {csv_dir_path}"
            f" --dst_dir {pq_dir_path}"
        )
        hsysinte.system(cmd)
        df = pd.read_parquet(os.path.join(pq_dir_path, "test.pq"))
        actual = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    def test2(self) -> None:
        """
        Test that --incremental option does not change the file.
        """
        csv_dir_path, pq_dir_path = self._generate_files()
        df = pd.read_parquet(os.path.join(pq_dir_path, "test.pq"))
        before = hunitest.convert_df_to_json_string(df, n_tail=None)
        cmd = (
            "im_v2/common/data/transform/csv_to_pq.py"
            f" --src_dir {csv_dir_path}"
            f" --dst_dir {pq_dir_path}"
            " --incremental"
        )
        hsysinte.system(cmd)
        df = pd.read_parquet(os.path.join(pq_dir_path, "test.pq"))
        after = hunitest.convert_df_to_json_string(df, n_tail=None)
        self.assert_equal(before, after)

    def _generate_files(self) -> Tuple[str,str]:
        test_dir = self.get_scratch_space()
        pq_dir_path = os.path.join(test_dir, "pq_dir")
        csv_dir_path = os.path.join(test_dir, "csv_dir")
        hio.create_dir(csv_dir_path, False)
        hio.create_dir(pq_dir_path, False)
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
        d2 = {        
            'timestamp':[1632646800000,1638646860000],
            'open':[49666.18,49666.63],
            'high':[49666.95,49666.98],
            'volume':[23.12681,61.99752],
            'low':[49666.45,49322.78],
            'close':[49230.63,49225.23],
            'currency_pair':["BTC_USDT","BTC_USDT"],
            'created_at':["2021-12-07 13:01:20.183463+00:00",
            "2021-12-07 13:01:20.183463+00:00"],
            'exchange_id':["binance","binance"]
        }
        df = pd.DataFrame(data=d)
        df.to_csv(os.path.join(csv_dir_path, "test.csv"), index=False)
        df2 = pd.DataFrame(data=d2)
        df2.to_parquet(os.path.join(pq_dir_path, "test.pq"), index=False)
        return csv_dir_path, pq_dir_path
