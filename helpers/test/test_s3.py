import logging
import os

import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_s3_1(hut.TestCase):
    def test_get_path1(self) -> None:
        file_path = "s3://alphamatic-data/data/kibot/All_Futures_Continuous_Contracts_daily"
        bucket_name, file_path = hs3.parse_path(file_path)
        self.assertEqual(bucket_name, "alphamatic-data")
        self.assertEqual(
            file_path, "data/kibot/All_Futures_Continuous_Contracts_daily"
        )

    def test_ls1(self) -> None:
        file_path = os.path.join(hs3.get_path(), "README.md")
        _LOG.debug("file_path=%s", file_path)
        # > aws s3 ls s3://alphamatic-data
        #                   PRE data/
        # 2021-04-06 1:17:44 48 README.md
        file_names = hs3.ls(file_path)
        self.assertGreater(len(file_names), 0)
