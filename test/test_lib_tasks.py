import logging

import helpers.dbg as dbg
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):

    def test_get_build_tag1(self) -> None:
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)
