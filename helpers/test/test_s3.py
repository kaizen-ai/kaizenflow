import configparser
import logging
import os

import pytest

import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_s3_get_credentials1(hut.TestCase):
    def test1(self) -> None:
        aws_profile = "am"
        res = hs3.get_aws_credentials(aws_profile)
        _LOG.debug("res=%s", str(res))

    def test2(self) -> None:
        aws_profile = "I don't exist"
        with self.assertRaises(configparser.NoSectionError) as cm:
            res = hs3.get_aws_credentials(aws_profile)
            _LOG.debug("res=%s", str(res))
        act = str(cm.exception)
        exp = r'''No section: "I don't exist"'''
        self.assert_equal(act, exp)


class Test_s3_1(hut.TestCase):
    def test_get_path1(self) -> None:
        s3_bucket = hs3.get_bucket()
        file_path = (
            f"s3://{s3_bucket}/data/kibot/All_Futures_Continuous_Contracts_daily"
        )
        bucket_name, file_path = hs3.parse_path(file_path)
        self.assertEqual(bucket_name, s3_bucket)
        self.assertEqual(
            file_path, "data/kibot/All_Futures_Continuous_Contracts_daily"
        )

    def test_ls1(self) -> None:
        file_path = os.path.join(hs3.get_path(), "README.md")
        _LOG.debug("file_path=%s", file_path)
        # > aws s3 ls s3://*****
        #                   PRE data/
        # 2021-04-06 1:17:44 48 README.md
        file_names = hs3.ls(file_path)
        self.assertGreater(len(file_names), 0)

    @pytest.mark.slow
    def test_listdir1(self) -> None:
        file_path = os.path.join(hs3.get_path(), "data/ib")
        file_names = hs3.listdir(file_path, mode="non-recursive")
        self.assertGreater(len(file_names), 0)
