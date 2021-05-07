import logging
import re
from typing import Dict

import ib_insync
import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# Start service with
# i docker_build_local_image && i im_tws_start_ib_interface --stage local --ib-app="TWS"

# Start docker_bash with
# i docker_bash --stage="local"
# ##> pytest instrument_master/ib/connect/test/test_im_tasks.py::TestImTwsStartIbInterface
class TestImTwsStartIbInterface(hut.TestCase):

    def test1(self) -> None:
        # Bring up the interface.
        ib_insync.util.logToConsole(logging.DEBUG)
        ib = ib_insync.IB()
        #port = 7492
        #ib.connect(port=7492)
        #ib.connect(port=4012)
        port = 4001
        print(port)
        #ib.connect(port=port)
        ib.connect(host="127.0.0.1", port=port, timeout=100)
