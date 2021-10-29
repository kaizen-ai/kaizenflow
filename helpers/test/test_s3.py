import logging
import os

import helpers.s3 as hs3
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_s3_get_credentials1(hunitest.TestCase):
    def test1(self) -> None:
        aws_profile = "am"
        res = hs3.get_aws_credentials(aws_profile)
        _LOG.debug("res=%s", str(res))


class Test_s3_functions1(hunitest.TestCase):
    def test_extract_bucket_from_path1(self) -> None:
        path = (
            "s3://alphamatic-data/tmp/TestCachingOnS3.test_with_caching1/joblib"
        )
        bucket, path = hs3.split_path(path)
        self.assert_equal(bucket, "alphamatic-data")
        self.assert_equal(path, "/tmp/TestCachingOnS3.test_with_caching1/joblib")


class Test_s3_1(hunitest.TestCase):
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
        _LOG.debug("file_path=%s", file_path)
        act = s3fs.exists(file_path)
        exp = True
        self.assertEqual(act, exp)

    def test_exists2(self) -> None:
        s3fs = hs3.get_s3fs("am")
        file_path = os.path.join(hs3.get_path(), "README_does_not_exist.md")
        _LOG.debug("file_path=%s", file_path)
        act = s3fs.exists(file_path)
        exp = False
        self.assertEqual(act, exp)

    def test_exists3(self) -> None:
        # > aws s3 ls alphamatic-data/data/ib/metadata/symbols-2021-04-01-143112738505.csv
        # 2021-04-26 08:39:00   61677776 symbols-2021-04-01-143112738505.csv
        s3fs = hs3.get_s3fs("am")
        file_path = os.path.join(
            hs3.get_path(), "data/ib/metadata/symbols-2021-04-01-143112738505.csv"
        )
        _LOG.debug("file_path=%s", file_path)
        act = s3fs.exists(file_path)
        exp = True
        self.assertEqual(act, exp)
