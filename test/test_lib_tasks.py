import logging
import re
from typing import Dict

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsi
import helpers.unit_test as hut

dbg.init_logger()
_LOG = logging.getLogger(__name__)

import lib_tasks as lib_tasks

class TestLibTasks1(hut.TestCase):

    def test_get_build_tag1(self):
        build_tag = lib_tasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)