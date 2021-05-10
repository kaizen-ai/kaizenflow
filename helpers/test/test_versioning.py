import logging
import os

import helpers.git as git
import helpers.unit_test as hut
import helpers.versioning as hversi

_LOG = logging.getLogger(__name__)


class TestVersioning1(hut.TestCase):
    def test_get_code_version1(self) -> None:
        git_root = git.get_client_root(super_module=False)
        file_name = os.path.join(git_root, "version.txt")
        code_version = hversi.get_code_version(file_name)
        _LOG.debug("code_version=%s", code_version)

    def test_get_code_version2(self) -> None:
        code_version = hversi.get_code_version()
        _LOG.debug("code_version=%s", code_version)

    def test_get_container_version1(self) -> None:
        container_version = hversi._get_container_version()
        _LOG.debug("container_version=%s", container_version)

    def test_check_version1(self) -> None:
        git_root = git.get_client_root(super_module=False)
        file_name = os.path.join(git_root, "version.txt")
        hversi.check_version(file_name)

    def test__check_version1(self) -> None:
        code_version = "amp-1.0.0"
        container_version = "amp-1.0.2"
        is_ok = hversi._check_version(code_version, container_version)
        self.assertFalse(is_ok)

    def test__check_version2(self) -> None:
        code_version = "amp-1.0.0"
        container_version = "amp-1.0.0"
        is_ok = hversi._check_version(code_version, container_version)
        self.assertTrue(is_ok)
