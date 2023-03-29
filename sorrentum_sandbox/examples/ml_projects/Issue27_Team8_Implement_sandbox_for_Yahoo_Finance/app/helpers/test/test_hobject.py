import abc
import logging
from typing import Any

import helpers.hdbg as hdbg
import helpers.hobject as hobject
import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


def _to_signature(self_: Any, obj: Any, **kwargs: Any) -> None:
    txt = []
    txt.append(hprint.frame("str:"))
    txt.append(hobject.obj_to_str(obj, **kwargs))
    txt.append(hprint.frame("repr:"))
    txt.append(hobject.obj_to_repr(obj, **kwargs))
    txt = "\n".join(txt)
    #
    self_.check_string(txt, purify_text=True)


class _Obj_to_str_TestCase(abc.ABC):
    @abc.abstractmethod
    def get_object(self) -> Any:
        ...

    def helper(self, **kwargs: Any) -> None:
        obj = self.get_object()
        hdbg.dassert_is_not(obj, None)
        _to_signature(self, obj, **kwargs)

    def test1(self) -> None:
        self.helper(attr_mode="__dict__")

    def test2(self) -> None:
        self.helper(attr_mode="dir")

    def test3(self) -> None:
        self.helper(print_type=True)

    def test4(self) -> None:
        self.helper(callable_mode="all")

    def test5(self) -> None:
        self.helper(private_mode="all")

    def test6(self) -> None:
        self.helper(dunder_mode="all")


# #############################################################################
# Test_obj_to_str1
# #############################################################################


class _Object1:
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


class Test_obj_to_str1(hunitest.TestCase, _Obj_to_str_TestCase):
    def get_object(self) -> Any:
        obj = _Object1()
        return obj


# #############################################################################
# Test_obj_to_str2
# #############################################################################


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

    def __repr__(self) -> str:
        return hobject.obj_to_str(self)


class _Object3:
    """
    Object storing another object.
    """

    def __init__(self) -> None:
        self.p = "p"
        self.q = "q"
        self.object2 = _Object2()


class Test_obj_to_str2(hunitest.TestCase, _Obj_to_str_TestCase):
    def get_object(self) -> Any:
        obj = _Object3()
        return obj