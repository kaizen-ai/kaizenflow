import logging

import helpers.dbg as dbg
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_dassert_eq1(ut.TestCase):

    def test1(self):
        dbg.dassert_eq(1, 1)

    def test2(self):
        dbg.dassert_eq(1, 1, msg="hello world")

    def test3(self):
        with self.assertRaises(AssertionError) as cm:
            dbg.dassert_eq(1, 2, msg="hello world")
        self.check_string(str(cm.exception))

    def test4(self):
        with self.assertRaises(AssertionError) as cm:
            dbg.dassert_eq(1, 2, "hello %s", "world")
        self.check_string(str(cm.exception))

    def test5(self):
        """
        Raise assertion with incorrect message.
        """
        with self.assertRaises(AssertionError) as cm:
            dbg.dassert_eq(1, 2, "hello %s")
        self.check_string(str(cm.exception))
