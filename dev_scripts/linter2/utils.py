import os
import re
from typing import List, Optional

import helpers.dbg as dbg
import helpers.io_ as io_


def write_file_back(file_name: str, txt: List[str], txt_new: List[str]) -> None:
    dbg.dassert_list_of_strings(txt)
    txt_as_str = "\n".join(txt)
    #
    dbg.dassert_list_of_strings(txt_new)
    txt_new_as_str = "\n".join(txt_new)
    #
    if txt_as_str != txt_new_as_str:
        io_.to_file(file_name, txt_new_as_str)


# #############################################################################

# TODO(gp): Move in a more general file: probably system_interaction.
def _is_under_dir(file_name: str, dir_name: str) -> bool:
    """Return whether a file is under the given directory."""
    subdir_names = file_name.split("/")
    return dir_name in subdir_names


def is_under_test_dir(file_name: str) -> bool:
    """Return whether a file is under a test directory (which is called
    "test")."""
    return _is_under_dir(file_name, "test")


def is_test_input_output_file(file_name: str) -> bool:
    """Return whether a file is used as input or output in a unit test."""
    ret = is_under_test_dir(file_name)
    ret &= file_name.endswith(".txt")
    return ret


def is_test_code(file_name: str) -> bool:
    """Return whether a file contains unit test code."""
    ret = is_under_test_dir(file_name)
    ret &= os.path.basename(file_name).startswith("test_")
    ret &= file_name.endswith(".py")
    return ret


def is_py_file(file_name: str) -> bool:
    """Return whether a file is a python file."""
    return file_name.endswith(".py")


def is_ipynb_file(file_name: str) -> bool:
    """Return whether a file is a jupyter notebook file."""
    return file_name.endswith(".ipynb")


def from_python_to_ipynb_file(file_name: str) -> str:
    dbg.dassert(is_py_file(file_name))
    ret = file_name.replace(".py", ".ipynb")
    return ret


def from_ipynb_to_python_file(file_name: str) -> str:
    dbg.dassert(is_ipynb_file(file_name))
    ret = file_name.replace(".ipynb", ".py")
    return ret


def is_paired_jupytext_file(file_name: str) -> bool:
    """Return whether a file is a paired jupytext file."""
    is_paired = (
        is_py_file(file_name)
        and os.path.exists(from_python_to_ipynb_file(file_name))
        or (
            is_ipynb_file(file_name)
            and os.path.exists(from_ipynb_to_python_file(file_name))
        )
    )
    return is_paired


def is_init_py(file_name: str) -> bool:
    return os.path.basename(file_name) == "__init__.py"


def is_separator(line: str) -> bool:
    """Check if the line matches a separator line.

    :return: True if it matches a separator line
    """
    return (
        line
        == "# #############################################################################"
    )


def is_shebang(line: str) -> bool:
    """Check if the line is a shebang (starts with #!)

    :return: True if it is a shebang (starts with #!)
    """
    return line.startswith("#!")


def parse_comment(
    line: str, regex: str = r"(^\s*)#\s*(.*)\s*"
) -> Optional[re.Match]:
    """Parse a line and return a comment if there's one.

    Seperator lines and shebang return None.
    """
    if is_separator(line) or is_shebang(line):
        return None

    return re.search(regex, line)
