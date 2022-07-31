import logging
from typing import Any

import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class _Object:
    """
    Object storing only scalar members but not other objects.
    """

    def __init__(self) -> None:
        self.a = False
        self.b = "hello"
        self.c = 3.14
        self._hello = "under"
        self.__hello = "double_dunder"
        self.hello = lambda x: x + 1


class _Object2:
    """
    Object using a `obj_to_str()` as repr.
    """

    def __init__(self) -> None:
        self.x = True
        self.y = "world"
        self.z = 6.28
        self._hello = "under"
        self.__hello = "double_dunder"
        self.hello = lambda x: x + 1

    def __repr__(self):
        return hprint.obj_to_str(self)


class _Object3:
    """
    Object storing another object.
    """

    def __init__(self) -> None:
        self.p = "p"
        self.q = "q"
        self.object2 = _Object2()


# #############################################################################
# Test_obj_to_str1
# #############################################################################

class Test_obj_to_str1(hunitest.TestCase):

    def helper(self, exp: str, **kwargs: Any) -> None:
        obj = _Object()
        act = hprint.obj_to_str(obj, **kwargs)
        self.assert_equal(act, exp, dedent=True, purify_text=True)

    def test1(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False'
          b='hello'
          c='3.14'
        """
        self.helper(exp, attr_mode="__dict__")

    def test2(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False'
          b='hello'
          c='3.14'
        """
        self.helper(exp, attr_mode="dir")

    def test3(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False' (<class 'bool'>)
          b='hello' (<class 'str'>)
          c='3.14' (<class 'float'>)
        """
        self.helper(exp, print_type=True)

    def test4(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False'
          b='hello'
          c='3.14'
          hello='<function _Object.__init__.<locals>.<lambda> at 0x>'
        """
        self.helper(exp, callable_mode="all")

    def test5(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False'
          b='hello'
          c='3.14'
          _hello='under'
        """
        self.helper(exp, private_mode="all")

    def test6(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object object at 0x>:
          a='False'
          b='hello'
          c='3.14'
          _Object__hello='double_dunder'
        """
        self.helper(exp, dunder_mode="all")


# #############################################################################
# Test_obj_to_str2
# #############################################################################


class Test_obj_to_str2(hunitest.TestCase):
    """
    Print an object using

    """

    def helper(self, exp: str, **kwargs: Any) -> None:
        obj = _Object2()
        act = hprint.obj_to_str(obj, **kwargs)
        self.assert_equal(act, exp, dedent=True, purify_text=True)

    def test1(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object2 object at 0x>:
          x='True'
          y='world'
          z='6.28'
        """
        self.helper(exp)


# #############################################################################
# Test_obj_to_str3
# #############################################################################


class Test_obj_to_str3(hunitest.TestCase):

    def helper(self, exp: str, **kwargs: Any) -> None:
        obj = _Object3()
        act = hprint.obj_to_str(obj, **kwargs)
        self.assert_equal(act, exp, dedent=True, purify_text=True)

    def test1(self) -> None:
        exp = r"""
        <helpers.test.test_hprint._Object3 object at 0x>:
          p='p'
          q='q'
          object2=
            <helpers.test.test_hprint._Object2 object at 0x>:
              x='True'
              y='world'
              z='6.28'
            """
        self.helper(exp)
