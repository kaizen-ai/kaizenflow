import logging

import pytest

import helpers.hunit_test as hunitest
import helpers.hversion as hversio

_LOG = logging.getLogger(__name__)


class TestVersioning1(hunitest.TestCase):
    def test_get_changelog_version1(self) -> None:
        """
        Test `cmamp` version.
        """
        code_version = hversio.get_changelog_version()
        _LOG.debug("code_version=%s", code_version)

    def test_get_changelog_version2(self) -> None:
        """
        Test `optimizer` version.
        """
        dir_name = "optimizer"
        code_version = hversio.get_changelog_version(dir_name)
        _LOG.debug("code_version=%s", code_version)

    def test_get_container_version1(self) -> None:
        container_version = hversio._get_container_version()
        _LOG.debug("container_version=%s", container_version)

    def test_check_version1(self) -> None:
        hversio.check_version()

    @pytest.mark.skip(reason="CmampTask570")
    def test__check_version1(self) -> None:
        code_version = "amp-1.0.0"
        container_version = "amp-1.0.2"
        is_ok = hversio._check_version(code_version, container_version)
        self.assertFalse(is_ok)

    def test__check_version2(self) -> None:
        code_version = "amp-1.0.0"
        container_version = "amp-1.0.0"
        is_ok = hversio._check_version(code_version, container_version)
        self.assertTrue(is_ok)
