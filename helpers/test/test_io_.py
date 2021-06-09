import logging
import os

import numpy as np
import pandas as pd

import helpers.csv_helpers as hchelp
import helpers.env as henv
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_load_df_from_json(hut.TestCase):
    def test1(self) -> None:
        test_json_path = os.path.join(self.get_input_dir(), "test.json")
        actual_result = hio.load_df_from_json(test_json_path)
        expected_result = pd.DataFrame(
            {
                "col1": ["a", "b", "c", "d"],
                "col2": ["a", "b", np.nan, np.nan],
                "col3": ["a", "b", "c", np.nan],
            }
        )
        actual_result = hprint.dataframe_to_str(actual_result)
        expected_result = hprint.dataframe_to_str(expected_result)
        self.assertEqual(actual_result, expected_result)
