import logging

import helpers.env as henv
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_env1(hunitest.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = henv.get_system_signature()
        _LOG.debug(txt)
