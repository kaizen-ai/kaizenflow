"""Code to automatically generate unit tests for functions.

Import as:

import helpers.playback as plbck
"""

import os
import inspect
import json
import logging
from typing import Any, List, Union, Optional, Tuple, Dict

import jsonpickle  # type: ignore
import jsonpickle.ext.pandas as jp_pd  # type: ignore
import pandas as pd
import collections as coll
import helpers.dbg as dbg
import helpers.io_ as io_

jp_pd.register_handlers()

_LOG = logging.getLogger(__name__)


# TODO(\*): Add more types.
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
        # Use jsonpickle to handle double quotes.
        output.append(jsonpickle.encode(obj))
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
    _tests_with_counts: Dict[str, int] = coll.defaultdict(int)

    def __init__(self, mode: str, to_file: Optional[bool] = None, max_tests: Optional[int] = None) -> None:
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
        self._code.append("")
        # Check if need to write the code directly to file.
        self._to_file = to_file if to_file is not None else False
        # Find filename to write the code.
        file_with_code = cur_frame.f_back.f_code.co_filename  # type: ignore
        dirname_with_code, filename_with_code = os.path.split(file_with_code)
        dirname_with_test = os.path.join(dirname_with_code, "test")
        self._test_file = os.path.join(dirname_with_test, "test_%s" % filename_with_code)
        # Create test file if it doesn't exist.
        if self._to_file and not os.path.exists(self._test_file):
            io_.create_dir(dirname_with_test, True)
            io_.to_file(self._test_file, "", mode = "w")
        # Get already existing content in the test file.
        if self._to_file:
            self._code = io_.from_file(self._test_file).split("\n")
        # Limit number of tests per tested function.
        self._max_tests = max_tests

    def run(self, func_output: Any) -> str:
        """Generate a unit test for the function.

        The unit test compares the actual function output with the expected
        `func_output`.

        :param func_output: the expected function output
        :return: the code of the unit test
        """
        # Count if we fixed max number of tests generated for a single function.
        if self._max_tests is not None:
            key = "%s-%s-%s" % (self._test_file, self._parent_class, self._func_name)
            if self.__class__._tests_with_counts.get(key, 0) >= self._max_tests:
                # Enough number of tests are generated.
                return ""
            # Increase counter since new test is coming.
            self.__class__._tests_with_counts[key] += 1
        self._add_imports()
        pointer = self._add_test_class()
        pointer = self._add_var_definitions(pointer)
        pointer = self._add_function_call(pointer)
        self._check_code(func_output, pointer)
        return self._gen_code()

    @staticmethod
    def test_code(output: str) -> None:
        # Try to execute in a fake environment.
        # ```
        # local_env = {}
        # _ = exec(output, local_env)
        # ```
        _ = exec(output)  # pylint: disable=exec-used

    def _check_code(self, func_output: Any, index_to_paste: int) -> int:
        check_code = []
        if self.mode == "check_string":
            if isinstance(func_output, (pd.DataFrame, pd.Series, str)):
                if not isinstance(func_output, str):
                    check_code.append(
                        "        act = hut.convert_df_to_string(act)"
                    )
            if not isinstance(func_output, (str, bytes)):
                check_code.append("        act = str(act)")
            check_code.append("        # Check output")
            check_code.append("        self.check_string(act)")
        elif self.mode == "assert_equal":
            check_code.append("        # Define expected output")
            func_output_as_code = to_python_code(func_output)
            check_code.append(f"        exp = {func_output_as_code}")
            if not isinstance(
                func_output, (int, float, str, list, dict, pd.DataFrame)
            ):
                check_code.append("        exp = jsonpickle.decode(exp)")

            if isinstance(func_output, (pd.DataFrame, pd.Series)):
                check_code.append("        act = hut.convert_df_to_string(act)")
                check_code.append("        exp = hut.convert_df_to_string(exp)")
            check_code.append("        # Compare actual and expected output")
            check_code.append("        self.assertEqual(act, exp)")
        else:
            raise ValueError("Invalid mode='%s'" % self.mode)
        # Put function call to the correct place.
        self._code[index_to_paste:index_to_paste] = check_code
        # Find index to paste next code.
        if index_to_paste == -1:
            return -1
        return index_to_paste + len(check_code)

    def _add_imports(self, additional: Union[None, List[str]] = None) -> None:
        # Construct what is needed to paste
        imports = []
        imports.append("import helpers.unit_test as hut")
        imports.append("import jsonpickle")
        imports.append("import pandas as pd")
        for a in additional or []:
            imports.append(a)
        # Remove what is already exists in the file.
        imports = self._filter_existing_lines(imports)
        index_to_paste = self._get_import_index()
        # If no imports are found need to add 2 newlines at the end.
        if index_to_paste == -1:
            imports.extend(["", ""])
        # Put imports to the correct place.
        self._code[index_to_paste:index_to_paste] = imports

    def _filter_existing_lines(self, new_lines: List[str]) -> List[str]:
        return [line for line in new_lines if line not in self._code]

    def _get_import_index(self) -> int:
        if not self._to_file:
            return -1
        import_indeces = [i for i, line in enumerate(self._code) if line.startswith("import")]
        if not import_indeces:
            # First import to paste.
            return -1
        last_import_index = max(import_indeces)
        return last_import_index + 1

    def _add_test_class(self) -> int:
        # Construct test class and test method.
        test_name = "".join([x.capitalize() for x in self._func_name.split("_")])
        header = []
        class_string = f"class Test{test_name}(hut.TestCase):"
        header.append(class_string)
        header = self._filter_existing_lines(header)
        if header:
            # First time we add a class with a current test.
            index_to_paste = -1
            header.append("    def test1(self) -> None:")
        else:
            # Find when class ends and count existing test methods.
            index_to_paste, count = self._get_class_index_count(class_string)
            header.append("    def test%i(self) -> None:" % (count + 1))
        # Put class or/and method definitions to the correct place.
        self._code[index_to_paste:index_to_paste] = header
        # Find index to paste the test code.
        next_iteration_index = -1
        if index_to_paste != -1:
            # Test is pasting at the middle of file.
            next_iteration_index = index_to_paste + len(header)
        return next_iteration_index

    def _get_class_index_count(self, class_string: str) -> Tuple[int, int]:
        class_start_index = self._code.index(class_string)
        class_end_index = -1
        # Find new class appearance.
        class_counter = 0
        method_counter = 0
        for line_number in range(class_start_index, len(self._code)):
            line = self._code[line_number]
            # Count methods.
            method_counter += line.startswith("    def test")
            # Count classes.
            class_counter += line.startswith("class ")
            # Current class is also counted.
            if class_counter == 2:
                # Next class is found, go back to a newline.
                class_end_index = line_number - 1
                break
        return (class_end_index, method_counter)

    def _add_function_call(self, index_to_paste: int) -> int:
        function_call = []
        function_call.append("        # Call function to test")
        if self._parent_class is None:
            fnc_call = [f"{k}={k}" for k in self._kwargs.keys()]
            function_call.append(
                "        act = %s(%s)" % (self._func_name, ", ".join(fnc_call))
            )
        else:
            var_code = to_python_code(self._parent_class)
            # Re-create the parent class.
            function_call.append(f"        cls = {var_code}")
            function_call.append("        cls = jsonpickle.decode(cls)")
            fnc_call = ["{0}={0}".format(k) for k in self._kwargs.keys()]
            # Call the method as a child of the parent class.
            function_call.append(
                f"        act = cls.{self._func_name}({', '.join(fnc_call)})"
            )
        # Put function call to the correct place.
        self._code[index_to_paste:index_to_paste] = function_call
        # Find index to paste next code.
        if index_to_paste == -1:
            return -1
        return index_to_paste + len(function_call)

    def _add_var_definitions(self, index_to_paste: int) -> int:
        var_definitions = []
        var_definitions.append("        # Define input variables")
        for key in self._kwargs:
            as_python = to_python_code(self._kwargs[key])
            var_definitions.append("        %s = %s" % (key, as_python))
            # Decode back to an actual Python object, if necessary.
            if not isinstance(
                self._kwargs[key], (int, float, str, list, dict, pd.DataFrame)
            ):
                var_definitions.append(
                    "        {0} = jsonpickle.decode({0})".format(key)
                )
        # Paste var definitions to the right place.
        self._code[index_to_paste:index_to_paste] = var_definitions
        # Find where to paste the next code.
        if index_to_paste == -1:
            return -1
        return index_to_paste + len(var_definitions)

    def _gen_code(self) -> str:
        code = "\n".join(self._code) + "\n"
        _LOG.debug("code=\n%s", code)
        if self._to_file:
            io_.to_file(self._test_file, code)
        return code


def filename(text: str) -> None:
    """Save a content to a generic file name"""
    frame = inspect.currentframe()
    func = frame.f_back.f_code.co_name
    import sys
    print(sys.modules[__name__])
    print(__name__)
    print(frame.f_back.f_code.co_filename)
    print(func)


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
