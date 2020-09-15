"""Code to automatically generate unit tests for functions.

Import as:

import helpers.playback as plbck
"""

import inspect
import json
import logging
from typing import Any, List, Union

import jsonpickle  # type: ignore
import jsonpickle.ext.pandas as jp_pd  # type: ignore
import pandas as pd

import helpers.dbg as dbg

jp_pd.register_handlers()

_LOG = logging.getLogger(__name__)


# TODO(\*): Unit test and add more types.
def to_python_code(obj: Any) -> str:
    """Serialize an object into a string of python code.

    :param obj: an object to serialize
    :return: a string of python code building the object
    """
    output = []
    if isinstance(obj, (int, float)):
        # Float 2.5 -> "2.5".
        output.append(str(obj))
    elif isinstance(obj, str):
        # String test -> '"test"'.
        output.append('"' + obj + '"')
    elif isinstance(obj, list):
        # List ["a", 1] -> '["a", 1]'.
        output_tmp = "["
        for el in obj:
            output_tmp += to_python_code(el) + ", "
        output_tmp = output_tmp.rstrip(", ") + "]"
        output.append(output_tmp)
    elif isinstance(obj, dict):
        # Dict {"a": 1} -> '{"a": 1}'.
        output_tmp = "{"
        for key in obj:
            output_tmp += (
                to_python_code(key) + ": " + to_python_code(obj[key]) + ", "
            )
        output_tmp = output_tmp.rstrip(", ") + "}"
        output.append(output_tmp)
    elif isinstance(obj, pd.DataFrame):
        # Dataframe with a column "a" and row values 1, 2 -> "pd.DataFrame.from_dict({'a':
        # [1, 2]})".
        vals = obj.to_dict(orient="list")
        output.append("pd.DataFrame.from_dict(%s)" % vals)
    else:
        # Use `jsonpickle` for serialization.
        _LOG.warning(
            "Type %s not found in serialization function: using jsonpickle.",
            type(obj),
        )
        output.append(f"r'{jsonpickle.encode(obj)}'")
    output = "\n".join(output)
    return output


class Playback:
    def __init__(self, mode: str) -> None:
        """Initialize the class variables.

        :param mode: the type of unit test to be generated (e.g. "assert_equal")
        """
        dbg.dassert_in(mode, ("check_string", "assert_equal"))
        self.mode = mode
        cur_frame = inspect.currentframe()
        self._func_name = cur_frame.f_back.f_code.co_name  # type: ignore
        # We can use kw arguments for all args. Python supports this.
        self._kwargs = cur_frame.f_back.f_locals.copy()  # type: ignore
        # It treats all arguments defined before itself as arguments. If this is done, it
        # will mess up the function call that will be created in `Playback.run`.
        expected_arg_count = cur_frame.f_back.f_code.co_argcount  # type: ignore
        if "kwargs" in self._kwargs:
            expected_arg_count += 1
        dbg.dassert_eq(
            expected_arg_count,
            len(cur_frame.f_back.f_locals),  # type: ignore
            msg="the Playback class should be the first thing instantiated in a function.",
        )
        # If the function is a method, store the parent class so we can also create that
        # in the test
        if "self" in self._kwargs:
            x = self._kwargs.pop("self")
            self._parent_class = x
            self._code = [
                f'# Test created for {cur_frame.f_back.f_globals["__name__"]}'  # type: ignore
                f".{x.__class__.__name__}.{self._func_name}"
            ]
        else:
            self._parent_class = None
            self._code = [
                # pylint: disable=line-too-long
                f'# Test created for {cur_frame.f_back.f_globals["__name__"]}.{self._func_name}'  # type: ignore
            ]

    def run(self, func_output: Any) -> str:
        """Generate a unit test for the function.

        The unit test compares the actual function output with the expected
        `func_output`.

        :param func_output: the expected function output
        :return: the code of the unit test
        """
        self._add_imports()
        self._add_test_class()
        self._add_var_definitions()
        self._add_function_call()
        self._check_code(func_output)
        return self._gen_code()

    @staticmethod
    def test_code(output: str) -> None:
        # Try to execute in a fake environment.
        # ```
        # local_env = {}
        # _ = exec(output, local_env)
        # ```
        _ = exec(output)  # pylint: disable=exec-used

    def _check_code(self, func_output: Any) -> None:
        if self.mode == "check_string":
            if isinstance(func_output, (pd.DataFrame, pd.Series, str)):
                if not isinstance(func_output, str):
                    self._code.append(
                        "        act = hut.convert_df_to_string(act)"
                    )
            if not isinstance(func_output, (str, bytes)):
                self._code.append("        act = str(act)")
            self._code.append("        # Check output")
            self._code.append("        self.check_string(act)")
        elif self.mode == "assert_equal":
            self._code.append("        # Define expected output")
            func_output_as_code = to_python_code(func_output)
            self._code.append(f"        exp = {func_output_as_code}")
            if not isinstance(
                func_output, (int, float, str, list, dict, pd.DataFrame)
            ):
                self._code.append("        exp = jsonpickle.decode(exp)")

            if isinstance(func_output, (pd.DataFrame, pd.Series)):
                self._code.append("        act = hut.convert_df_to_string(act)")
                self._code.append("        exp = hut.convert_df_to_string(exp)")
            self._code.append("        # Compare actual and expected output")
            self._code.append("        self.assertEqual(act, exp)")
        else:
            raise ValueError("Invalid mode='%s'" % self.mode)

    def _add_imports(self, additional: Union[None, List[str]] = None) -> None:
        self._code.append("import helpers.unit_test as hut")
        self._code.append("import jsonpickle")
        self._code.append("import pandas as pd")
        for a in additional or []:
            self._code.append(a)
        self._code.extend(["", ""])

    def _add_test_class(self) -> None:
        test_name = "".join([x.capitalize() for x in self._func_name.split("_")])
        self._code.append(f"class Test{test_name}(hut.TestCase):")
        self._code.append("    def test1(self) -> None:")

    def _add_function_call(self) -> None:
        self._code.append("        # Call function to test")
        if self._parent_class is None:
            fnc_call = [f"{k}={k}" for k in self._kwargs.keys()]
            self._code.append(
                "        act = %s(%s)" % (self._func_name, ", ".join(fnc_call))
            )
        else:
            var_code = to_python_code(self._parent_class)
            # Re-create the parent class.
            self._code.append(f"        cls = {var_code}")
            self._code.append("        cls = jsonpickle.decode(cls)")
            fnc_call = ["{0}={0}".format(k) for k in self._kwargs.keys()]
            # Call the method as a child of the parent class.
            self._code.append(
                f"        act = cls.{self._func_name}({', '.join(fnc_call)})"
            )

    def _add_var_definitions(self) -> None:
        self._code.append("        # Define input variables")
        for key in self._kwargs:
            as_python = to_python_code(self._kwargs[key])
            self._code.append("        %s = %s" % (key, as_python))
            # Decode back to an actual Python object, if necessary.
            if not isinstance(
                self._kwargs[key], (int, float, str, list, dict, pd.DataFrame)
            ):
                self._code.append(
                    "        {0} = jsonpickle.decode({0})".format(key)
                )

    def _gen_code(self) -> str:
        code = "\n".join(self._code) + "\n"
        _LOG.debug("code=\n%s", code)
        return code


def json_pretty_print(parsed: Any) -> str:
    """Pretty print a json object.

    :param parsed: a json object
    :return: a prettified json object
    """
    if isinstance(parsed, str):
        parsed = json.loads(parsed)
    # `ret = pprint.pformat(parsed)
    ret = json.dumps(parsed, indent=4, sort_keys=True)
    return ret


def round_trip_convert(obj1: Any, log_level: int) -> Any:
    """Encode and decode with `jsonpickle` ensuring the object remains the
    same.

    :param obj1: the initial object
    :param log_level: the level of logging
    :return: the object after encoding and decoding
    """
    _LOG.log(log_level, "# obj1=\n%s", obj1)
    _LOG.log(log_level, "class=%s", type(obj1))
    # Encode.
    frozen = jsonpickle.encode(obj1)
    _LOG.log(log_level, "# frozen=\n%s", json_pretty_print(frozen))
    # Decode.
    obj2 = jsonpickle.decode(frozen)
    _LOG.log(log_level, "# obj2=\n%s", obj2)
    _LOG.log(log_level, "class=%s", type(obj1))
    # Check whether the decoded version is the same as the initial object.
    if str(type(obj1)).startswith("<class '"):
        # TODO(gp): Check the str representation.
        pass
    else:
        if isinstance(obj1, pd.DataFrame):
            dbg.dassert(obj1.equals(obj2), "obj1=\n%s\nobj2=\n%s", obj1, obj2)
        else:
            dbg.dassert_eq(obj1, obj2)
    return obj2
