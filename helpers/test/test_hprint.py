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
        self._hello = "under"
        self.__hello = "double_dunder"
        self.hello = lambda x: x + 1


class Test_obj_to_str1(hunitest.TestCase):

    def helper(self, exp: str, **kwargs: Any) -> None:
        a = False
        b = "hello"
        c = 3.14
        obj = _Object(a, b, c)
        act = hprint.obj_to_str(obj, **kwargs)
        self.assert_equal(act, exp, dedent=True, purify_text=True)

    def test1(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        """
        self.helper(exp, attr_mode="__dict__")

    def test2(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        """
        self.helper(exp, attr_mode="dir")

    def test3(self) -> None:
        exp = r"""
        a='False' (<class 'bool'>)
        b='hello' (<class 'str'>)
        c='3.14' (<class 'float'>)
        """
        self.helper(exp, print_type=True)

    def test4(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        hello='<function _Object.__init__.<locals>.<lambda> at 0x>'
        """
        self.helper(exp, callable_mode="all")

    def test5(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        _hello='under'
        """
        self.helper(exp, private_mode="all")

    def test6(self) -> None:
        exp = r"""
        a='False'
        b='hello'
        c='3.14'
        _Object__hello='double_dunder'
        """
        self.helper(exp, dunder_mode="all")
