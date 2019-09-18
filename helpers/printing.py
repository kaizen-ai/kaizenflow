import helpers.dbg as dbg

"""
This module should be imported as `prnt`.
"""

# #############################################################################
# Debug output
# #############################################################################

COLOR_MAP = {
    "blue": "\033[94m",
    "green": "\033[92m",
    "none": "\033[0m",
    "purple": "\033[95m",
    "red": "\033[91m",
    "yellow": "\033[93m",
}


def color_highlight(text, color):

    return COLOR_MAP[color] + text + COLOR_MAP["none"]


def clear_screen():
    print((chr(27) + "[2J"))


def line(char=None, num_chars=None):
    """
    Return a line with the desired character and

    :param char:
    :param num_chars:
    :return:
    """
    char = "#" if char is None else char
    num_chars = 80 if num_chars is None else num_chars
    return char * num_chars


def frame(message, char1=None, num_chars=None, char2=None, thickness=1):
    """
    Print a frame around a message.

    :param message:
    :param char1:
    :param num_chars:
    :param char2:
    :param thickness:
    :return:
    """
    # Fill in the default values.
    if char1 is None:
        # User didn't specify any char.
        char1 = char2 = "#"
    elif char1 is not None and char2 is None:
        # User specified only one char.
        char2 = char1
    elif char1 is None and char2 is not None:
        # User specified the second char, but not the first.
        dbg.dfatal("Invalid char1='%s' char2='%s'" % (char1, char2))
    else:
        # User specified both chars. Nothing to do.
        pass
    num_chars = 80 if num_chars is None else num_chars
    # Sanity check.
    dbg.dassert_lte(1, thickness)
    dbg.dassert_eq(len(char1), 1)
    dbg.dassert_eq(len(char2), 1)
    dbg.dassert_lte(1, num_chars)
    # Build the return value.
    ret = (
        (line(char1, num_chars) + "\n") * thickness
        + message
        + "\n"
        + (line(char2, num_chars) + "\n") * thickness
    ).rstrip("\n")
    return ret


# TODO(gp): -> indent
def space(str_, num_spaces=2):
    """
    Add "num_spaces" spaces before each line of the string str_.
    """
    return prepend(str_, " " * num_spaces)


def dedent(txt):
    """
    Remove all extra leadning / trailing spaces and empty lines
    """
    txt_out = []
    for curr_line in txt.split("\n"):
        curr_line = curr_line.rstrip(" ").lstrip(" ")
        if curr_line:
            txt_out.append(curr_line)
    return "\n".join(txt_out)


def prepend(str_, prefix):
    """
    Add "prefix" before each line of the string str_.
    """
    # lines = ["<" + prefix + curr_line + ">" for curr_line in str_.split("\n")]
    lines = [prefix + curr_line for curr_line in str_.split("\n")]
    return "\n".join(lines)


def vars_to_debug_string(vars_as_str, locals_):
    """
    Create a string with var name -> var value.
    E.g., ["var1", "var2"] is converted into:
        var1=...
        var2=...
    """
    txt = []
    for var in vars_as_str:
        txt.append(var + "=")
        txt.append(space(str(locals_[var])))
    return "\n".join(txt)


# #############################################################################
# Pretty print data structures.
# #############################################################################


def thousand_separator(v):
    v = "{0:,}".format(v)
    return v


def perc(a, b, invert=False, num_digits=2, use_thousands_separator=False):
    """
    Calculate percentage a / b as a string.

    Asserts 0 <= a <= b. If true, returns a/b to `num_digits` decimal places.

    :param a: numerator
    :param b: denominator
    :param invert: assume the fraction is (b - a) / b
        This is useful when we want to compute the complement of a count.
    :param use_thousands_separator: report the numbers using thousands separator
    :return: string with a/b
    """
    dbg.dassert_lte(0, a)
    dbg.dassert_lte(a, b)
    if use_thousands_separator:
        a_str = str("{0:,}".format(a))
        b_str = str("{0:,}".format(b))
    else:
        a_str = str(a)
        b_str = str(b)
    if invert:
        a = b - a
    dbg.dassert_lte(0, num_digits)
    fmt = "%s / %s = %." + str(num_digits) + "f%%"
    ret = fmt % (a_str, b_str, float(a) / b * 100.0)
    return ret


def round_digits(v, num_digits=2, use_thousands_separator=False):
    """
    Round digit returning a string representing the formatted number.

    :param v: value to convert
    :param num_digits: number of digits to represent v on
            None is (Default value = 2)
    :param use_thousands_separator: use "," to separate thousands (Default value = False)
    :returns: str with formatted value
    """
    if (num_digits is not None) and isinstance(v, float):
        fmt = "%0." + str(num_digits) + "f"
        res = float(fmt % v)
    else:
        res = v
    if use_thousands_separator:
        res = "{0:,}".format(res)
    res = str(res)
    return res


# #############################################################################


def format_list(v, sep=" ", max_n=None, tag=None):
    sep = " "
    # sep = ", "
    if max_n is None:
        max_n = 10
    n = len(v)
    txt = ""
    if tag is not None:
        txt += "%s: " % tag
    txt += "(%s) " % n
    if n < max_n:
        txt += sep.join(map(str, v))
    else:
        txt += sep.join(map(str, v[: max_n / 2]))
        txt += " ... "
        txt += sep.join(map(str, v[(-max_n) / 2 :]))
    return txt


# TODO(gp): Use format_list().
def list_to_str(l, tag="", sort=False, axis=0, to_string=False):
    """
    Print list / index horizontally or vertically.
    """
    # TODO(gp): Fix this.
    _ = to_string
    txt = ""
    if axis == 0:
        if l is None:
            txt += "%s: (%s) %s" % (tag, 0, "None") + "\n"
        else:
            # dbg.dassert_in(type(l), (list, pd.Index, pd.Int64Index))
            vals = list(map(str, l))
            if sort:
                vals = sorted(vals)
            txt += "%s: (%s) %s" % (tag, len(l), " ".join(vals)) + "\n"
    elif axis == 1:
        txt += "%s (%s):" % (tag, len(l)) + "\n"
        vals = list(map(str, l))
        if sort:
            vals = sorted(vals)
        txt += "\n".join(vals) + "\n"
    else:
        raise ValueError("Invalid axis='%s'" % axis)
    return txt


# TODO(gp): -> set_diff_to_str
def print_set_diff(
    obj1, obj2, obj1_name="obj1", obj2_name="obj2", add_space=False
):
    def _to_string(obj):
        return " ".join(map(str, obj))

    print("# %s vs %s" % (obj1_name, obj2_name))
    obj1 = set(obj1)
    dbg.dassert_lte(1, len(obj1))
    print("* %s: (%s) %s" % (obj1_name, len(obj1), _to_string(obj1)))
    if add_space:
        print()
    #
    obj2 = set(obj2)
    dbg.dassert_lte(1, len(obj2))
    print("* %s: (%s) %s" % (obj2_name, len(obj2), _to_string(obj2)))
    if add_space:
        print()
    #
    intersection = obj1.intersection(obj2)
    print("* intersect=(%s) %s" % (len(intersection), _to_string(intersection)))
    if add_space:
        print()
    #
    diff = obj1 - obj2
    print("* %s-%s=(%s) %s" % (obj1_name, obj2_name, len(diff), _to_string(diff)))
    if add_space:
        print()
    #
    diff = obj2 - obj1
    print("* %s-%s=(%s) %s" % (obj2_name, obj1_name, len(diff), _to_string(diff)))
    if add_space:
        print()


def dataframe_to_str(
    df, max_columns=10000, max_colwidth=2000, max_rows=500, display_width=10000
):
    import pandas as pd

    with pd.option_context(
        "display.max_colwidth",
        max_colwidth,
        #'display.height', 1000,
        "display.max_rows",
        max_rows,
        "display.max_columns",
        max_columns,
        "display.width",
        display_width,
    ):
        res = str(df)
    return res


# #############################################################################
# Notebook output
# #############################################################################

# TODO(gp): Move to explore.py


def config_notebook():
    import pandas as pd
    import matplotlib.pyplot as plt

    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)
    # plt.rcParams
    plt.rcParams["figure.figsize"] = (20, 5)
    plt.rcParams["legend.fontsize"] = 14
    plt.rcParams["font.size"] = 14
    plt.rcParams["image.cmap"] = "rainbow"
