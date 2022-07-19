import logging
import os
from typing import Any

import pytest

import helpers.henv as henv
import helpers.hgit as hgit
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# #############################################################################
# TestRepoConfig_Amp
# #############################################################################


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
        actual = henv.execute_repo_config_code("get_name()")
        self.assertEqual(actual, self.expected_repo_name)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_repo_name3(self) -> None:
        """
        If //amp is a supermodule, then repo_config should report something
        different than //amp.
        """
        actual = henv.execute_repo_config_code("get_name()")
        self.assertNotEqual(actual, self.expected_repo_name)

    def test_config_func_to_str(self) -> None:
        _LOG.info(henv.execute_repo_config_code("config_func_to_str()"))


# #############################################################################
# TestRepoConfig_Amp_signature
# #############################################################################


# Copied from repo_config.py


def is_dev_ck() -> bool:
    # sysname='Darwin'
    # nodename='gpmac.lan'
    # release='19.6.0'
    # version='Darwin Kernel Version 19.6.0: Mon Aug 31 22:12:52 PDT 2020;
    #   root:xnu-6153.141.2~1/RELEASE_X86_64'
    # machine='x86_64'
    host_name = os.uname()[1]
    host_names = ("dev1", "dev2")
    am_host_name = os.environ.get("AM_HOST_NAME")
    _LOG.debug("host_name=%s am_host_name=%s", host_name, am_host_name)
    is_dev_ck_ = host_name in host_names or am_host_name in host_names
    return is_dev_ck_


def is_dev4() -> bool:
    """
    Return whether it's running on dev4.
    """
    host_name = os.uname()[1]
    dev4 = "cf-spm-dev4"
    am_host_name = os.environ.get("AM_HOST_NAME")
    _LOG.debug("host_name=%s am_host_name=%s", host_name, am_host_name)
    is_dev4_ = dev4 in (host_name, am_host_name)
    return is_dev4_


def is_mac() -> bool:
    host_os_name = os.uname()[0]
    am_host_os_name = os.environ.get("AM_HOST_OS_NAME")
    _LOG.debug(
        "host_os_name=%s am_host_os_name=%s", host_os_name, am_host_os_name
    )
    is_mac_ = host_os_name == "Darwin" or am_host_os_name == "Darwin"
    return is_mac_


def get_repo_short_name() -> str:
    dir_name = "."
    include_host_name = False
    repo_name = hgit.get_repo_full_name_from_dirname(dir_name, include_host_name)
    _LOG.debug("repo_name=%s", repo_name)
    # ck/cmamp
    short_repo_name = repo_name.split("/")[1]
    _LOG.debug("short_repo_name=%s", short_repo_name)
    return short_repo_name


# End copy.


def _execute_only_in_target_repo(target_name: str) -> None:
    repo_short_name = get_repo_short_name()
    if repo_short_name != target_name:
        pytest.skip(f"Only run on {target_name} (and not {repo_short_name})")


def _execute_only_on_dev_ck() -> None:
    is_dev_ck_ = is_dev_ck()
    if not is_dev_ck_:
        pytest.skip("Only run on dev CK")


def _execute_only_on_mac() -> None:
    is_mac_ = is_mac()
    if not is_mac_:
        pytest.skip("Only run on Mac")


def _check(self: Any, exp: str) -> None:
    act = henv.env_to_str(add_system_signature=False)
    act = hunitest.filter_text("get_name", act)
    act = hunitest.filter_text("get_repo_map", act)
    act = hunitest.filter_text("AM_HOST_NAME", act)
    self.assert_equal(act, exp, fuzzy_match=True)


# > pytest ./amp/helpers/test/test_repo_config_amp.py


class TestRepoConfig_Cmamp_signature1(hunitest.TestCase):
    def test_dev1_server(self) -> None:
        target_name = "cmamp"
        _execute_only_in_target_repo(target_name)
        #
        _execute_only_on_dev_ck()
        #
        exp = r"""
        # Repo config:
          enable_privileged_mode='False'
          get_docker_base_image_name='cmamp'
          get_docker_shared_group=''
          get_docker_user=''
          get_host_name='github.com'
          get_invalid_words='[]'
          get_shared_data_dirs='{'/data/shared': '/shared_data'}'
          has_dind_support='False'
          has_docker_sudo='True'
          is_AM_S3_available='True'
          is_CK_S3_available='True'
          is_dev4='False'
          is_dev_ck='True'
          is_inside_ci='False'
          is_inside_docker='True'
          is_mac='False'
          run_docker_as_root='False'
          skip_submodules_test='True'
          use_docker_network_mode_host='True'
          use_docker_sibling_containers='False'
        # Env vars:
          AM_AWS_ACCESS_KEY_ID=undef
          AM_AWS_DEFAULT_REGION=undef
          AM_AWS_PROFILE='am'
          AM_AWS_S3_BUCKET='alphamatic-data'
          AM_AWS_SECRET_ACCESS_KEY=undef
          AM_ECR_BASE_PATH='665840871993.dkr.ecr.us-east-1.amazonaws.com'
          AM_ENABLE_DIND='0'
          AM_FORCE_TEST_FAIL=''
          AM_HOST_OS_NAME='Linux'
          AM_PUBLISH_NOTEBOOK_LOCAL_PATH=''
          AM_REPO_CONFIG_CHECK='True'
          AM_REPO_CONFIG_PATH=''
          AM_TELEGRAM_TOKEN=***
          CI=''
          GH_ACTION_ACCESS_TOKEN=empty
        """
        _check(self, exp)

    def test_mac(self) -> None:
        target_name = "cmamp"
        _execute_only_in_target_repo(target_name)
        #
        _execute_only_on_mac()
        #
        exp = r"""
        # Repo config:
          enable_privileged_mode='True'
          get_docker_base_image_name='cmamp'
          get_docker_shared_group=''
          get_docker_user=''
          get_host_name='github.com'
          get_invalid_words='[]'
          get_shared_data_dirs='None'
          has_dind_support='True'
          has_docker_sudo='True'
          is_AM_S3_available='True'
          is_CK_S3_available='True'
          is_dev4='False'
          is_dev_ck='False'
          is_inside_ci='False'
          is_inside_docker='True'
          is_mac='True'
          run_docker_as_root='False'
          skip_submodules_test='True'
          use_docker_network_mode_host='True'
          use_docker_sibling_containers='False'
          use_main_network='False'
        # Env vars:
          AM_AWS_ACCESS_KEY_ID=undef
          AM_AWS_DEFAULT_REGION=undef
          AM_AWS_PROFILE='am'
          AM_AWS_S3_BUCKET='alphamatic-data'
          AM_AWS_SECRET_ACCESS_KEY=undef
          AM_ECR_BASE_PATH='665840871993.dkr.ecr.us-east-1.amazonaws.com'
          AM_ENABLE_DIND='1'
          AM_FORCE_TEST_FAIL=''
          AM_HOST_OS_NAME='Darwin'
          AM_PUBLISH_NOTEBOOK_LOCAL_PATH=''
          AM_REPO_CONFIG_CHECK='True'
          AM_REPO_CONFIG_PATH=''
          AM_TELEGRAM_TOKEN=***
          CI=''
          GH_ACTION_ACCESS_TOKEN=empty
        """
        _check(self, exp)


class TestRepoConfig_Amp_signature1(hunitest.TestCase):
    def test_dev1_server(self) -> None:
        target_name = "amp"
        _execute_only_in_target_repo(target_name)
        #
        _execute_only_on_dev_ck()
        #
        exp = r"""
        # Repo config:
          enable_privileged_mode='False'
          get_docker_base_image_name='cmamp'
          get_docker_shared_group=''
          get_docker_user=''
          get_host_name='github.com'
          get_invalid_words='[]'
          get_shared_data_dirs='{'/data/shared': '/shared_data'}'
          has_dind_support='False'
          has_docker_sudo='True'
          is_AM_S3_available='True'
          is_CK_S3_available='True'
          is_dev4='False'
          is_dev_ck='True'
          is_inside_ci='False'
          is_inside_docker='True'
          is_mac='False'
          run_docker_as_root='False'
          skip_submodules_test='True'
          use_docker_network_mode_host='True'
          use_docker_sibling_containers='False'
        # Env vars:
          AM_AWS_ACCESS_KEY_ID=undef
          AM_AWS_DEFAULT_REGION=undef
          AM_AWS_PROFILE='am'
          AM_AWS_S3_BUCKET='alphamatic-data'
          AM_AWS_SECRET_ACCESS_KEY=undef
          AM_ECR_BASE_PATH='665840871993.dkr.ecr.us-east-1.amazonaws.com'
          AM_ENABLE_DIND='0'
          AM_FORCE_TEST_FAIL=''
          AM_HOST_OS_NAME='Linux'
          AM_PUBLISH_NOTEBOOK_LOCAL_PATH=''
          AM_REPO_CONFIG_CHECK='True'
          AM_REPO_CONFIG_PATH=''
          AM_TELEGRAM_TOKEN=***
          CI=''
          GH_ACTION_ACCESS_TOKEN=empty
        """
        _check(self, exp)

    def test_mac(self) -> None:
        target_name = "amp"
        _execute_only_in_target_repo(target_name)
        #
        _execute_only_on_mac()
        #
        exp = r"""
        # Repo config:
          enable_privileged_mode='True'
          get_docker_base_image_name='cmamp'
          get_docker_shared_group=''
          get_docker_user=''
          get_host_name='github.com'
          get_invalid_words='[]'
          get_shared_data_dirs='None'
          has_dind_support='True'
          has_docker_sudo='True'
          is_AM_S3_available='True'
          is_CK_S3_available='True'
          is_dev4='False'
          is_dev_ck='False'
          is_inside_ci='False'
          is_inside_docker='True'
          is_mac='True'
          run_docker_as_root='False'
          skip_submodules_test='True'
          use_docker_network_mode_host='True'
          use_docker_sibling_containers='False'
          use_main_network='False'
        # Env vars:
          AM_AWS_ACCESS_KEY_ID=undef
          AM_AWS_DEFAULT_REGION=undef
          AM_AWS_PROFILE='am'
          AM_AWS_S3_BUCKET='alphamatic-data'
          AM_AWS_SECRET_ACCESS_KEY=undef
          AM_ECR_BASE_PATH='665840871993.dkr.ecr.us-east-1.amazonaws.com'
          AM_ENABLE_DIND='1'
          AM_FORCE_TEST_FAIL=''
          AM_HOST_OS_NAME='Darwin'
          AM_PUBLISH_NOTEBOOK_LOCAL_PATH=''
          AM_REPO_CONFIG_CHECK='True'
          AM_REPO_CONFIG_PATH=''
          AM_TELEGRAM_TOKEN=***
          CI=''
          GH_ACTION_ACCESS_TOKEN=empty
        """
        _check(self, exp)
