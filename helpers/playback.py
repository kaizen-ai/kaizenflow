"""
Code to generate automatically unit tests for code.

Import as:

import helpers.playback as plbck
"""

import json
import logging

# TODO(gp): Add to conda env.
import jsonpickle

import helpers.dbg as dbg
# Register the pandas handler.
import jsonpickle.ext.pandas as jsonpickle_pd
jsonpickle_pd.register_handlers()
#import jsonpickle.ext.numpy as jsonpickle_numpy
import pandas as pd

_LOG = logging.getLogger(__name__)


# TODO: Unit test and add more types.
def to_python_code(obj):
    output = []
    if isinstance(obj, (int, float, str)):
        #output.append(str(type(obj)) + "(" + str(obj) + ")")
        output.append(str(obj))
    elif isinstance(obj, list):
        output_tmp = "["
        for l in obj:
            output_tmp += to_python_code(l) + ", "
        output_tmp += "]"
        output.append(output_tmp)
    elif isinstance(obj, pd.DataFrame):
        vals = obj.to_dict(orient="list")
        output.append("pd.DataFrame.from_dict(%s)" % vals)
        # TODO: If there is a name of the data frame
        # output.append("df.name = "..."))
    else:
        _LOG.warning("...")
        # use jsonpickle.
    output = "\n".join(output)
    return output


# TODO: Pass the name of the unit test class.
# TODO: Add option to generate input files instead of inlining variables.
# TODO: Always use jsonpickle
class Playback:

    def __init__(self, mode, func_name, *args, **kwargs):
        """
        :param mode: the type of unit test to be generated
        :param func_name: the name of the function to test
        :param args: the positional parameters for the function to test
        :param kwargs: the keyword parameters for the function to test
        """
        dbg.dassert_in(mode, ("check_string", "assert_equal"))
        self.mode = mode
        # TODO(gp): We can infer the name of the function automatically.
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs

    def start(self):
        """
        Sample the inputs to the function under test.
        """
        self.json_args = [jsonpickle.encode(x) for x in self.args]
        self.json_kwargs = {x[0]: jsonpickle.encode(x[1]) for x in self.kwargs.items()}

    def end(self, ret):
        """
        Sample the output of the function under test.

        :param ret:
        """
        # Encode the json to construct the expected variable.
        self.ret_json = jsonpickle.encode(ret)
        #
        code = []
        code.append("# Initialize values for unit test.")
        # Name of the variables to assign to the function.
        var_names = []
        # TODO: Add boilerplate for unit test.
        # class TestPlaybackInputOutput1(hut.TestCase):
        #
        #     def test1(self) -> None:
        # For positional parameters we need to generate dummy variables.
        if self.json_args:
            prefix_var_name = "dummy_"
            for i, json_param in enumerate(self.json_args):
                var_name = prefix_var_name + str(i)
                code.append("%s = r'%s'" % (var_name, json_param))
                code.append("{0} = jsonpickle.decode({0})".format(var_name))
                var_names.append(var_name)
        if self.json_kwargs:
            for key in self.json_kwargs:
                code.append("%s = r'%s'" % (key, self.json_kwargs[key]))
                code.append("{0} = jsonpickle.decode({0})".format(key))
                var_names.append(key)
        code.append("# Call function.")
        code.append("act = %s(%s)" % (self.func_name, ', '.join(var_names)))
        code.append("# Create expected value of function output.")
        # TODO(gp): Factor out this idiom.
        code.append("exp = r'%s'" % self.ret_json)
        code.append("exp = jsonpickle.decode(exp)")
        code.append("# Check.")
        if self.mode == "assert_equal":
            # Add a different check for different values.
            if isinstance(ret, pd.DataFrame):
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
    def test_code(output: str):
        # Try to execute in a fake environment.
        #local_env = {}
        #_ = exec(output, local_env)
        _ = exec(output)



def json_pretty_print(parsed):
    """
    Pretty print a json object.
    """
    if isinstance(parsed, str):
        parsed = json.loads(parsed)
    #ret = pprint.pformat(parsed)
    ret = json.dumps(parsed, indent=4, sort_keys=True)
    return ret


def round_trip_convert(obj1, log_level):
    """
    Encode and decode with JSON ensuring the object is the same.
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
    # Check whether the decoded version is the same as the intial object.
    if str(type(obj1)).startswith("<class '"):
        # TODO(gp): Check the str representation.
        pass
    else:
        if isinstance(obj1, pd.DataFrame):
            dbg.dassert(obj1.equals(obj2), "obj1=\n%s\nobj2=\n%s", obj1, obj2)
        else:
            dbg.dassert_eq(obj1, obj2)
    return obj2
