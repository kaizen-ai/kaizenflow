import os

import helpers.hunit_test as hunitest


class TestOutcomePurificationFunctions(hunitest.TestCase):
    def test_purify_from_env_vars1(self) -> None:
        """
        Test the process of CK AWS putification.
        """
        env_folder = os.environ["CK_AWS_S3_BUCKET"]
        path = f"s3://{env_folder}/"
        #
        act = hunitest.purify_from_env_vars(path)
        exp = "s3://$CK_AWS_S3_BUCKET/"
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_purify_from_env_vars2(self) -> None:
        """
        Test the process of Telegram token putification.
        """
        env_folder = os.environ["AM_TELEGRAM_TOKEN"]
        path = f"s3://{env_folder}/"
        #
        act = hunitest.purify_from_env_vars(path)
        exp = "s3://$AM_TELEGRAM_TOKEN/"
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_purify_from_env_vars3(self) -> None:
        """
        Test the process of AM AWS putification.
        """
        env_folder = os.environ["AM_AWS_S3_BUCKET"]
        path = f"s3://{env_folder}/"
        #
        act = hunitest.purify_from_env_vars(path)
        exp = "s3://$AM_AWS_S3_BUCKET/"
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_purify_from_env_vars4(self) -> None:
        """
        Test the process of AM ECR putification.
        """
        env_folder = os.environ["AM_ECR_BASE_PATH"]
        path = f"s3://{env_folder}/"
        #
        act = hunitest.purify_from_env_vars(path)
        exp = "s3://$AM_ECR_BASE_PATH/"
        self.assert_equal(act, exp, fuzzy_match=True)
