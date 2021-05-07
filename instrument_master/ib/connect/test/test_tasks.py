import logging
import re
from typing import Dict

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

class TestImTwsStartIbInterface(hut.TestCase):

    def test1(self) -> None:
        # Bring up the interface.
        # TODO: Need the IB container
        import ib_insync

        ib = ib_insync.IB()
        ib.connect(port=7492)

