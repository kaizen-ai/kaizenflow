import helpers.dbg as dbg

# TODO(gp): Rename print_


def perc(a, b, use_thousands_separator=False):
    """
    Calculate percentage a / b as a string.

    Asserts 0 <= a <= b. If true, returns a/b to two decimal places.

    :param a: numerator
    :param b: denominator
    :param use_thousands_separator: Default value = False)
    :returns: a/b to two decimal places.

    """
    dbg.dassert_lte(0, a)
    dbg.dassert_lte(a, b)
    if use_thousands_separator:
        a_str = str("{0:,}".format(a))
        b_str = str("{0:,}".format(b))
    else:
        a_str = str(a)
        b_str = str(b)
    ret = "%s / %s = %.2f%%" % (a_str, b_str, float(a) / b * 100.0)
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


def thousand_separator(v):
    v = "{0:,}".format(v)
    return v


def list_to_string(l):
    ret = "(%s) %s" % (len(l), ", ".join(l))
    return ret


class Colors(object):
    PURPLE = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    NONE = '\033[0m'


INFO = Colors.BLUE + "INFO: " + Colors.NONE
WARNING = Colors.PURPLE + "WARNING: " + Colors.NONE
ERROR = Colors.RED + "ERROR: " + Colors.NONE


def clear_screen():
    print((chr(27) + "[2J"))


def line(char=None, numChars=None):
    char = "#" if char is None else char
    numChars = 80 if numChars is None else numChars
    return char * numChars


def frame(message, char1=None, numChars=None, char2=None, thickness=1):
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
    numChars = 80 if numChars is None else numChars
    # Sanity check.
    dbg.dassert_lte(1, thickness)
    dbg.dassert_eq(len(char1), 1)
    dbg.dassert_eq(len(char2), 1)
    dbg.dassert_lte(1, numChars)
    # Build the return value.
    ret = ((line(char1, numChars) + "\n") * thickness + message + "\n" +
           (line(char2, numChars) + "\n") * thickness).rstrip("\n")
    return ret


# TODO(gp): -> indent
def space(str_, numSpaces=2):
    """
    Add "numSpaces" spaces before each line of the string str_.
    """
    return prepend(str_, " " * numSpaces)


def prepend(str_, prefix):
    """
    Add "prefix" before each line of the string str_.
    """
    #lines = ["<" + prefix + line + ">" for line in str_.split("\n")]
    lines = [prefix + line for line in str_.split("\n")]
    return "\n".join(lines)


# TODO(gp): Move all these params as default.
def dataframe_to_str(df,
                     max_columns=10000,
                     max_colwidth=2000,
                     max_rows=500,
                     display_width=10000):
    import pandas as pd
    with pd.option_context(
            "display.max_colwidth", max_colwidth,
            #'display.height', 1000,
            'display.max_rows', max_rows,
            'display.max_columns', max_columns,
            'display.width', display_width):
        res = str(df)
    return res