"""
Import as:

import helpers.playback as plbck
"""

# - DONE: Handle complex types with jsonpickle (e.g., pandas data frame)
# - IN PROGRESS: Unit test the unit tester
# - Generalize for args, kwargs
#   
# - Create code to make it look like a ParticleOne unit test
# - Improve the serialization by generating "nicer" code directly, instead of
#   using jsonpicle.
# - Make it a decorator

import jsonpickle

#def serialize_to_python_code(obj: Any):
#    if isinstance(obj, pd.DataFrame):
#        ret = []
#        ret.append("pd.DataFrame(...")
#    elif isinstance(obj, pd.Series):
#        ret = []
#        ret.append("pd.Series(...")
#    elif isinstance(obj, 


class Playback:

    #def __init__(self, file_name, mode, *args, **kwargs):
    # self.args = args
    # self.kwargs = kwargs
    def __init__(self, file_name, mode, func_name, a, b):
        self.a = a
        self.b = b

    def start(self):
        self.a_json = jsonpickle.encode(self.a)
        self.b_json = jsonpickle.encode(self.b)

    def end(self, ret):
        self.ret_json = jsonpickle.encode(ret)
        output = []
        output.append("# Initialize values for unit test.")
        #output.append("a = %s" % jsonpickle.decode(self.a_json))
        #output.append("b = %s" % jsonpickle.decode(self.b_json))
        output.append("a = r'%s'" % self.a_json)
        output.append("a = jsonpickle.decode(a)")
        output.append("b = r'%s'" % self.b_json)
        output.append("b = jsonpickle.decode(b)")
        output.append("# Apply values.")
        #output.append("act = F(a, b)[1]")
        output.append("act = F(a, b)")
        output.append("exp = r'%s'" % self.ret_json)
        output.append("exp = jsonpickle.decode(exp)")
        #output.append("self.assertEqual(act, exp)")
        output.append("assert act.equals(exp)")
        #output.append("assert act == exp")
        output = "\n".join(output)
        return output

    @staticmethod
    def test_code(output: str):
        # Try to execute in a fake environment.
        #local_env = {}
        #_ = exec(output, local_env)
        _ = exec(output)


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
