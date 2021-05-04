import logging

import helpers.unit_test as hut
import helpers.version as hversi


_LOG = logging.getLogger(__name__)


class TestVersion1(hut.TestCase):
    def test_get_code_version1(self) -> None:
        code_version = hversi.get_code_version()
        _LOG.debug("code_version=%s", code_version)

    def test_get_container_version1(self) -> None:
        container_version = hversi._get_container_version()
        _LOG.debug("container_version=%s", container_version)

    def test_check_version1(self) -> None:
        hversi.check_version()

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

