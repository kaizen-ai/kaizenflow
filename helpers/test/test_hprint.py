import logging
from typing import Any

import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class _Object:

    def __init__(self, a: bool, b: str, c: float) -> None:
        self.a = a
        self.b = b
        self.c = c
        self._hello = "dunder"
        self.__hello = "double_dunder"


class Test_obj_to_str1(hunitest.TestCase):

    def _helper(self, exp: str, **kwargs: Any) -> None:
        a = False
        b = "hello"
        c = 3.14
        obj = _Object(a, b, c)
        act = hprint.obj_to_str(obj, **kwargs)
        self.assert_equal(act, exp, dedent=True)

    def test1(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        """
        self._helper(exp, attr_mode="__dict__")

    def test2(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        """
        self._helper(exp, attr_mode="dir")

    def test3(self) -> None:
        exp = r"""
        a=False (<class 'bool'>)
        b=hello (<class 'str'>)
        c=3.14 (<class 'float'>)
        """
        self._helper(exp, print_type=True)
