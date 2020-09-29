import datetime
import logging
from typing import Any

import pandas as pd

import helpers.playback as plbck
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestJsonRoundtrip1(hut.TestCase):
    """Test roundtrip conversion through jsonpickle for different types."""

    def test1(self) -> None:
        obj = 3
        #
        plbck.round_trip_convert(obj, logging.DEBUG)

    def test2(self) -> None:
        obj = "hello"
        #
        plbck.round_trip_convert(obj, logging.DEBUG)

    def test3(self) -> None:
        data = {
            "Product": ["Desktop Computer", "Tablet", "iPhone", "Laptop"],
            "Price": [700, 250, 800, 1200],
        }
        df = pd.DataFrame(data, columns=["Product", "Price"])
        df.index.name = "hello"
        #
        obj = df
        plbck.round_trip_convert(obj, logging.DEBUG)

    def test4(self) -> None:
        obj = datetime.date(2015, 1, 1)
        #
        plbck.round_trip_convert(obj, logging.DEBUG)


class TestPlaybackInputOutput1(hut.TestCase):
    """Freeze the output of Playback."""

    def test1(self) -> None:
        """Test for int inputs."""
        # Create inputs.
        a = 3
        b = 2
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test2(self) -> None:
        """Test for string inputs."""
        # Create inputs.
        a = "test"
        b = "case"
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test3(self) -> None:
        """Test for list inputs."""
        # Create inputs.
        a = [1, 2, 3]
        b = [4, 5, 6]
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test4(self) -> None:
        """Test for dict inputs."""
        # Create inputs.
        a = {"1": 2}
        b = {"3": 4}
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test5(self) -> None:
        """Test for pd.DataFrame inputs."""
        # Create inputs.
        a = pd.DataFrame({"Price": [700, 250, 800, 1200]})
        b = pd.DataFrame({"Price": [1, 1, 1, 1]})
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test6(self) -> None:
        """Test for datetime.date inputs (using `jsonpickle`)."""
        # Create inputs.
        a = datetime.date(2015, 1, 1)
        b = datetime.date(2012, 1, 1)
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def test7(self) -> None:
        """Test for int inputs with check_string."""
        # Create inputs.
        a = 3
        b = 2
        # Generate, freeze and execute a unit test.
        self._helper("check_string", a=a, b=b)

    def test8(self) -> None:
        """Test for string inputs with check_string."""
        # Create inputs.
        a = "test"
        b = "case"
        # Generate, freeze and execute a unit test.
        self._helper("check_string", a=a, b=b)

    def test9(self) -> None:
        """Test for list inputs with check_string."""
        # Create inputs.
        a = [1, 2, 3]
        b = [4, 5, 6]
        # Generate, freeze and execute a unit test.
        self._helper("check_string", a=a, b=b)

    def test10(self) -> None:
        """Test for dict inputs with check_string."""
        # Create inputs.
        a = {"1": 2}
        b = {"3": 4}
        # Generate, freeze and execute a unit test.
        self._helper("check_string", a=a, b=b)

    def test11(self) -> None:
        """Test for pd.DataFrame inputs with check_string."""
        # Create inputs.
        a = pd.DataFrame({"Price": [700, 250, 800, 1200]})
        b = pd.DataFrame({"Price": [1, 1, 1, 1]})
        # Generate, freeze and execute a unit test.
        self._helper("check_string", a=a, b=b)

    def test12(self) -> None:
        """Test for dict inputs with data structures recursion."""
        # Create inputs.
        a = {"1": ["a", 2]}
        b = {"3": pd.DataFrame({"Price": [700, 250, 800, 1200]}), "4": {"5": 6}}
        # Generate, freeze and execute a unit test.
        self._helper("assert_equal", a=a, b=b)

    def _helper(self, mode: str, *args: Any, **kwargs: Any) -> None:
        # Define a function to generate a unit test for.
        def get_result_ae(a: Any, b: Any) -> Any:
            p = plbck.Playback("assert_equal")
            if isinstance(a, datetime.date) and isinstance(b, datetime.date):
                return p.run(abs(a - b))
            if isinstance(a, dict) and isinstance(b, dict):
                c = {}
                c.update(a)
                c.update(b)
                return p.run(c)
            return p.run(a + b)

        def get_result_cs(a: Any, b: Any) -> Any:
            p = plbck.Playback("check_string")
            if isinstance(a, datetime.date) and isinstance(b, datetime.date):
                return p.run(abs(a - b))
            if isinstance(a, dict) and isinstance(b, dict):
                c = {}
                c.update(a)
                c.update(b)
                return p.run(c)
            return p.run(a + b)

        if mode == "assert_equal":
            code = get_result_ae(*args, **kwargs)
        elif mode == "check_string":
            code = get_result_cs(*args, **kwargs)
        else:
            raise ValueError("Invalid mode ")
        self.check_string(code)
        _LOG.debug("Testing code:\n%s", code)
        exec(code, locals())  # pylint: disable=exec-used


class TestToPythonCode1(hut.TestCase):
    """Test to_python_code() for different types."""

    def _check(self, input_obj: Any, expected: str) -> None:
        res = plbck.to_python_code(input_obj)
        self.assert_equal(res, expected)

    def test_float1(self) -> None:
        """Test float without first zero"""
        self._check(0.1, "0.1")

    def test_float2(self) -> None:
        """Test positive float"""
        self._check(1.0, "1.0")

    def test_float3(self) -> None:
        """Test negative float"""
        self._check(-1.1, "-1.1")

    def test_int1(self) -> None:
        """Test zero"""
        self._check(0, "0")

    def test_int2(self) -> None:
        """Test positive int"""
        self._check(10, "10")

    def test_int3(self) -> None:
        """Test negative int"""
        self._check(-10, "-10")

    def test_str1(self) -> None:
        """Test str simple"""
        self._check("a", '"a"')

    def test_str2(self) -> None:
        """Test str with double quotes"""
        self._check('"b"', '"\\"b\\""')

    def test_str3(self) -> None:
        """Test str with single quotes"""
        self._check("'c'", "\"'c'\"")

    def test_list1(self) -> None:
        """Test List"""
        self._check([1, 0.2, "3"], '[1, 0.2, "3"]')

    def test_dict1(self) -> None:
        """Test Dist"""
        self._check({"a": 0.2, 3: "b"}, '{"a": 0.2, 3: "b"}')

    def test_df1(self) -> None:
        """Test pd.DataFrame (single quotes expected in field names)"""
        self._check(
            pd.DataFrame.from_dict({"a": [0.2, 0.1]}),
            "pd.DataFrame.from_dict({'a': [0.2, 0.1]})",
        )
