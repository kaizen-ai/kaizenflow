"""
Import as:

import helpers.playback as plbck
"""

import jsonpickle
import jsonpickle.ext.pandas as jsonpickle_pd
import jsonpickle.ext.numpy as jsonpickle_numpy
jsonpickle_pd.register_handlers()


class Playback:
    # def __init__(self, file_name, mode, *args, **kwargs):
    # self.args = args
    # self.kwargs = kwargs
    import string
    letters = list(string.ascii_lowercase)

    def __init__(self, file_name, mode, func_name, *args, **kwargs):
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs


    def start(self):
        self.json_args = [jsonpickle.encode(x) for x in self.args]
        self.json_kwargs = {x[0]: jsonpickle.encode(x[1]) for x in self.kwargs.items()}

    def type_identification(self, output):
        letter_ind = 0
        variables = []
        if self.json_args:
            for json_param in self.json_args:
                output.append("%s = r'%s'" % (self.letters[letter_ind], json_param))
                output.append("{0} = jsonpickle.decode({0})".format(self.letters[letter_ind]))
                variables.append(self.letters[letter_ind])
                letter_ind += 1
        if self.json_kwargs:
            for key in self.json_kwargs:
                output.append("%s = r'%s'" % (key, self.json_kwargs[key]))
                output.append("{0} = jsonpickle.decode({0})".format(key))
                variables.append(key)
        return variables


    def end(self, ret):
        self.ret_json = jsonpickle.encode(ret)
        output = []
        output.append("# Initialize values for unit test.")
        variables = self.type_identification(output)
        output.append("# Apply values.")
        output.append("act = %s(%s)" % (self.func_name, ', '.join(variables)))
        output.append("exp = r'%s'" % self.ret_json)
        output.append("exp = jsonpickle.decode(exp)")
        output.append("assert act.equals(exp)")
        output = "\n".join(output)
        print("output=", output)
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
