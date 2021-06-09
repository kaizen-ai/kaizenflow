import logging
import os

import numpy as np
import pandas as pd

import helpers.csv_helpers as hchelp
import helpers.env as henv
import helpers.io_ as hio
import helpers.printing as hprint
import helpers.s3 as hs3
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_env1(hut.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = henv.get_system_signature()
        _LOG.debug(txt)
