import logging
import unittest.mock as umock
from typing import Any

import pytest

import helpers.hprint as hprint
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)

_to_str = hprint.to_str

# References
# - https://docs.python.org/3/library/unittest.mock.html
# - https://realpython.com/python-mock-library/

# #############################################################################

# Mocks are used to imitate objects in the code base and need to have the same
# interface of objects they are replacing.


def _check(self: Any, str_to_eval: str, exp_val: str) -> None:
    """
    Evaluate `str_to_eval` and compare it to expected value `exp_val`.
    """
    # The variable lives 3 levels in the stack trace from here.
    act_val = _to_str(str_to_eval, frame_lev=3)
    _LOG.debug("%s", act_val)
    self.assert_equal(act_val, exp_val, purify_text=True)


class Test_Mock1(hunitest.TestCase):
    """
    - A `Mock` creates attributes / methods as you access them
    - The return value of a mocked attribute / method is also a `Mock`
    """

    def check(self, *args, **kwargs):
        _check(self, *args, **kwargs)

    def test_lazy_attributes1(self):
        obj = umock.Mock()
        # obj is a Mock object.
        self.check("obj", "obj=<Mock id='xxx'>")
        # Calling an attribute creates a Mock.
        self.check("obj.a", "obj.a=<Mock name='mock.a' id='xxx'>")
        # Assigning an attribute in the mock creates an attribute.
        obj.a = 3
        self.check("obj.a", "obj.a=3")

    def test_lazy_methods1(self):
        # Mock json module `import json`.
        json = umock.Mock()
        self.check("json", "json=<Mock id='xxx'>")
        # Create a function on the fly that returns a mock.
        v = json.dumps()
        self.assertTrue(isinstance(v, umock.Mock))
        self.check("json.dumps", "json.dumps=<Mock name='mock.dumps' id='xxx'>")
        # The mocked function and the returned value from a mock function are
        # different mocks.
        self.check("v", "v=<Mock name='mock.dumps()' id='xxx'>")
        self.check("type(v)", "type(v)=<class 'unittest.mock.Mock'>")
        self.check(
            "json.dumps()", "json.dumps()=<Mock name='mock.dumps()' id='xxx'>"
        )
        self.assertTrue(isinstance(json.dumps, umock.Mock))
        self.assertNotEqual(id(v), id(json.dumps))

    def test_assert1(self):
        json = umock.Mock()
        json.loads("hello")
        # Check that the mocked function was called as expected.
        json.loads.assert_called()
        json.loads.assert_called_once()
        json.loads.assert_called_with("hello")
        self.assertEqual(json.loads.call_count, 1)
