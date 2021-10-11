import logging

import helpers.env as henv
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


class Test_env1(huntes.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = henv.get_system_signature()
        _LOG.debug(txt)
