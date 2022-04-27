import logging

import pytest

import helpers.hgit as hgit
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestRepoConfig_Amp(hunitest.TestCase):

    expected_repo_name = "//amp"

    def test_repo_name1(self) -> None:
        """
        Show that when importing repo_config, one doesn't get necessarily the
        outermost repo_config (e.g., for lime one gets amp.repo_config).
        """
        import repo_config

        actual = repo_config.get_name()
        _LOG.info(
            "actual=%s expected_repo_name=%s", actual, self.expected_repo_name
        )

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_repo_name2(self) -> None:
        """
        If //amp is a supermodule, then repo_config should report //amp.
        """
        actual = hgit.execute_repo_config_code("get_name()")
        self.assertEqual(actual, self.expected_repo_name)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_repo_name3(self) -> None:
        """
        If //amp is a supermodule, then repo_config should report something
        different than //amp.
        """
        actual = hgit.execute_repo_config_code("get_name()")
        self.assertNotEqual(actual, self.expected_repo_name)

    def test_config_func_to_str(self) -> None:
        _LOG.info(hgit.execute_repo_config_code("config_func_to_str()"))
