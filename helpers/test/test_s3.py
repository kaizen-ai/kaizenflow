import configparser
import logging
import os

import pytest

import helpers.s3 as hs3
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_s3_get_credentials1(hut.TestCase):
    def test1(self) -> None:
        aws_profile = "am"
        res = hs3.get_aws_credentials(aws_profile)
        _LOG.debug("res=%s", str(res))


class Test_s3_1(hut.TestCase):

    def test_ls1(self) -> None:
        file_path = os.path.join(hs3.get_path(), "README.md")
        _LOG.debug("file_path=%s", file_path)
        # > aws s3 ls s3://*****
        #                   PRE data/
        # 2021-04-06 1:17:44 48 README.md
        s3fs = hs3.get_s3fs("am")
        file_names = s3fs.ls(file_path)
        _LOG.debug("file_names=%s", file_names)
        self.assertGreater(len(file_names), 0)

    def test_glob1(self) -> None:
        # > aws s3 ls s3://alphamatic-data/data/ib/metadata/
        # 2021-04-26 08:39:00      18791 exchanges-2021-04-01-134738089177.csv
        # 2021-04-26 08:39:00      18815 exchanges-2021-04-01-143112738505.csv
        # 2021-04-26 08:39:00   61677776 symbols-2021-04-01-134738089177.csv
        # 2021-04-26 08:39:00   61677776 symbols-2021-04-01-143112738505.csv
        s3fs = hs3.get_s3fs("am")
        file_path = os.path.join(hs3.get_path(), "data/ib/metadata")
        glob_pattern = file_path + "/exchanges-*"
        _LOG.debug("glob_pattern=%s", glob_pattern)
        file_names = s3fs.glob(glob_pattern)
        _LOG.debug("file_names=%s", file_names)
        self.assertGreater(len(file_names), 0)

    def test_exists1(self) -> None:
        s3fs = hs3.get_s3fs("am")
        file_path = os.path.join(hs3.get_path(), "README.md")
        act = s3fs.exists(file_path)
        exp = True
        self.assertEqual(act, exp)

    def test_exists2(self) -> None:
        s3fs = hs3.get_s3fs("am")
        file_path = os.path.join(hs3.get_path(), "README_does_not_exist.md")
        act = s3fs.exists(file_path)
        exp = False
        self.assertEqual(act, exp)
