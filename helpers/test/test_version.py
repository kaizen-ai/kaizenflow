import logging

# import helpers.dbg as dbg
import helpers.unit_test as hut

# import pytest

import helpers.version as hvers


_LOG = logging.getLogger(__name__)


class TestVersion1(hut.TestCase):

    def test_get_code_version1(self) -> None:
        code_version = hvers.get_code_version()
        _LOG.debug("code_version=%s", code_version)

    def test_get_container_version1(self) -> None:
        container_version = hvers.get_container_version()
        _LOG.debug("container_version=%s", container_version)

    def test_check_version1(self) -> None:
        code_version = "1.0.0"
        container_version = "1.0.2"
        with self.assertRaises(RuntimeError) as cm:
            hvers._check_version(code_version, container_version)
        act = str(cm.exception)
        self.check_string(act)

    def test_check_version2(self) -> None:
        hvers.check_version()
