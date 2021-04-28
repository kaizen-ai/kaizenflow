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
        hvers.check_version()
