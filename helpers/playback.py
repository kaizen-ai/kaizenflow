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

_LOG = logging.getLogger(__name__)

class Playback:

    def __init__(self, mode, func_name, *args, **kwargs):
        """
        :param mode: the type of unit test to be generated
        :param func_name: the name of the function to test
        :param args: the positional parameters for the function to test
        :param kwargs: the keyword parameters for the function to test
        """
        # Register the pandas handler.
        jsonpickle.ext.pandas.register_handlers()
        #import jsonpickle.ext.numpy as jsonpickle_numpy
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
        # For positional parameters we need to generate dummy variables.
        if self.json_args:
            prefix_var_name = "dummy_"
            for i, json_param in enumerate(self.json_args):
                var_name = prefix_var_name + str(i)
                output.append("%s = r'%s'" % (var_name, json_param))
                output.append("{0} = jsonpickle.decode({0})".format(json_param))
                var_names.append(var_name)
        if self.json_kwargs:
            for key in self.json_kwargs:
                output.append("%s = r'%s'" % (key, self.json_kwargs[key]))
                output.append("{0} = jsonpickle.decode({0})".format(key))
                var_names.append(key)
        code.append("# Call function.")
        code.append("act = %s(%s)" % (self.func_name, ', '.join(variables)))
        code.append("# Create expected value of function output.")
        # TODO(gp): Factor out this idiom.
        code.append("exp = r'%s'" % self.ret_json)
        code.append("exp = jsonpickle.decode(exp)")
        code.append("# Check.")
        if self.mode == "assert_equal":
            # TODO(gp): Add a different check for different values.
            code.append("assert act.equals(exp)")
        else:
            raise ValueError("Invalid mode='%s'" % self.mode)
        #
        code = "\n".join(code)
        #_LOG.debug("output=", code)
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


#
## def F(a: int, b: int):
##     c = {}
##     c["pavel"] = a + b
##     return c
#
#use_playback = True
#
#def F(a: pd.DataFrame, b: pd.DataFrame):
#    if use_playback:
#        playback = Playback("", "", "F", a, b)
#        playback.start()
#    #c = {}
#    #c["pavel"] = a + b
#    c = a + b
#    if use_playback:
#        output = playback.end(c)
#        res = output, c
#    else:
#        res = c
#    return res
#
#a = pd.DataFrame(
# {
#    'Price': [700, 250, 800, 1200]
#})
#b = pd.DataFrame(
# {
#    'Price': [1, 1, 1, 1]
#})
#
#res = F(a, b)
#output = res[0]
#print(output)
#exec(output)
