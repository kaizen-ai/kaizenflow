"""Code to automatically generate unit tests for functions.

Import as:

import helpers.playback as plbck
"""

import json
import logging
from typing import Any

import jsonpickle  # type: ignore
# Register the pandas handler.
import jsonpickle.ext.pandas as jsonpickle_pd  # type: ignore
import pandas as pd

import helpers.dbg as dbg

jsonpickle_pd.register_handlers()

_LOG = logging.getLogger(__name__)


# TODO: Unit test and add more types.
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
        for l in obj:
            output_tmp += to_python_code(l) + ", "
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
        # Dataframe with a column "a" and row values 1, 2 ->
        # "pd.DataFrame.from_dict({'a': [1, 2]})".
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


# TODO: Pass the name of the unit test class.
# TODO: Add option to generate input files instead of inlining variables.
class Playback:
    def __init__(
        self, mode: str, func_name: str, *args: Any, **kwargs: Any
    ) -> None:
        """Initialize the class variables.

        :param mode: the type of unit test to be generated (e.g. "assert_equal")
        :param func_name: the name of the function to test
        :param args: the positional parameters for the function to test
        :param kwargs: the keyword parameters for the function to test
        :return:
        """
        dbg.dassert_in(mode, ("check_string", "assert_equal"))
        self.mode = mode
        # TODO(gp): We can infer the name of the function automatically.
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs

    def run(self, func_output: Any) -> str:
        """Generate a unit test for the function.

        The unit test compares the actual function output with the expected
        `func_output`.

        :param func_output: the expected function output
        :return: the code of the unit test
        """
        code = []
        code.append("# Initialize function parameters.")
        # To store the names of the variables, to which function parameters are
        # assigned.
        var_names = []
        # TODO: Add boilerplate for unit test.
        # class TestPlaybackInputOutput1(hut.TestCase):
        #
        #     def test1(self) -> None:
        # For positional parameters we need to generate dummy variables.
        if self.args:
            prefix_var_name = "param"
            for i, param_obj in enumerate(self.args):
                # The variables will be called "param0", "param1", etc.
                var_name = prefix_var_name + str(i)
                var_names.append(var_name)
                # Serialize the object into a string of python code.
                var_code = to_python_code(param_obj)
                code.append("%s = %s" % (var_name, var_code))
                if not isinstance(
                    param_obj, (int, float, str, list, dict, pd.DataFrame)
                ):
                    # Decode the jsonpickle encoding.
                    code.append("{0} = jsonpickle.decode({0})".format(var_name))
        if self.kwargs:
            for key in self.kwargs:
                var_names.append(key)
                # Serialize the object into a string of python code.
                var_code = to_python_code(self.kwargs[key])
                code.append("%s = %s" % (key, var_code))
                if not isinstance(
                    self.kwargs[key], (int, float, str, list, dict, pd.DataFrame)
                ):
                    # Decode the jsonpickle encoding.
                    code.append("{0} = jsonpickle.decode({0})".format(key))
        # Add to the code the function call that generates the actual output.
        code.append("# Get the actual function output.")
        code.append("act = %s(%s)" % (self.func_name, ", ".join(var_names)))
        # Add to the code the serialization of the expected output.
        code.append("# Create the expected function output.")
        func_output_code = to_python_code(func_output)
        code.append("exp = %s" % func_output_code)
        if not isinstance(
            func_output, (int, float, str, list, dict, pd.DataFrame)
        ):
            # Decode the jsonpickle encoding.
            code.append("exp = jsonpickle.decode(exp)")
        # Add to the code the equality check between actual and expected.
        code.append("# Check whether the expected value equals the actual value.")
        if self.mode == "assert_equal":
            # Add a different check for different values.
            if isinstance(func_output, pd.DataFrame):
                code.append("assert act.equals(exp)")
            else:
                code.append("assert act == exp")
        else:
            raise ValueError("Invalid mode='%s'" % self.mode)
        #
        code = "\n".join(code)
        _LOG.debug("code=\n%s", code)
        return code

    @staticmethod
    def test_code(output: str) -> None:
        # Try to execute in a fake environment.
        # local_env = {}
        # _ = exec(output, local_env)
        _ = exec(output)  # pylint: disable=exec-used


def json_pretty_print(parsed: Any) -> str:
    """Pretty print a json object.

    :param parsed: a json object
    :return: a prettified json object
    """
    if isinstance(parsed, str):
        parsed = json.loads(parsed)
    # ret = pprint.pformat(parsed)
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
