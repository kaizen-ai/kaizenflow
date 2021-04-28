import logging

import helpers.dbg as dbg
import helpers.unit_test as hut

dbg.init_logger()
_LOG = logging.getLogger(__name__)

import lib_tasks as ltasks


class TestLibTasks1(hut.TestCase):
    def test_get_build_tag1(self):
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)
