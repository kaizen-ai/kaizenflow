import glob
import logging
import os

import pytest

import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# ##############################################################################
# pandoc.py
# ##############################################################################


# TODO(gp): Generalize to all users, or at least Jenkins.
@pytest.mark.skipif('si.get_user_name() != "saggese"')
class Test_pandoc1(ut.TestCase):
    def _helper(self, in_file, action):
        exec_path = git.find_file_in_git_tree("pandoc.py")
        dbg.dassert_exists(exec_path)
        #
        tmp_dir = self.get_scratch_space()
        out_file = os.path.join(tmp_dir, "output.pdf")
        cmd = []
        cmd.append(exec_path)
        cmd.append("-a %s" % action)
        cmd.append("--tmp_dir %s" % tmp_dir)
        cmd.append("--input %s" % in_file)
        cmd.append("--output %s" % out_file)
        cmd.append("--no_open")
        cmd.append("--no_gdrive")
        cmd.append("--no_cleanup")
        cmd = " ".join(cmd)
        si.system(cmd)
        # Check.
        if action == "pdf":
            out_file = os.path.join(tmp_dir, "tmp.pandoc.tex")
        elif action == "html":
            out_file = os.path.join(tmp_dir, "tmp.pandoc.html")
        else:
            raise ValueError("Invalid action='%s'" % action)
        act = io_.from_file(out_file, split=False)
        return act

    def test1(self):
        """
        Convert one txt file to PDF and check that the .tex file is as expected.
        """
        file_name = "code_style.txt.test"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        #
        act = self._helper(file_name, "pdf")
        self.check_string(act)

    def test2(self):
        """
        Convert one txt file to HTML and check that the .tex file is as expected.
        """
        file_name = "code_style.txt.test"
        file_name = os.path.join(
            self.get_input_dir(test_method_name="test1"), file_name
        )
        file_name = os.path.abspath(file_name)
        #
        act = self._helper(file_name, "html")
        self.check_string(act)

    def test_all_notes(self):
        """
        Convert to pdf all the notes in docs/notes.
        """
        git_dir = git.get_client_root(super_module=False)
        dir_name = os.path.join(git_dir, "docs/notes/*.txt")
        file_names = glob.glob(dir_name)
        for file_name in file_names:
            _LOG.debug(prnt.frame("file_name=%s" % file_name))
            self._helper(file_name, "html")


# ##############################################################################
# preprocess_md_for_pandoc.py
# ##############################################################################


def _run_preprocess(in_file: str, out_file: str) -> str:
    """
    Execute the end-to-end flow for preprocess_md_for_pandoc.py returning
    the output as string.
    """
    exec_path = git.find_file_in_git_tree("preprocess_md_for_pandoc.py")
    dbg.dassert_exists(exec_path)
    #
    dbg.dassert_exists(in_file)
    #
    cmd = []
    cmd.append(exec_path)
    cmd.append("--input %s" % in_file)
    cmd.append("--output %s" % out_file)
    cmd = " ".join(cmd)
    si.system(cmd)
    # Check.
    act = io_.from_file(out_file, split=False)
    return act


class Test_preprocess1(ut.TestCase):
    """
    Check that the output of preprocess_md_for_pandoc.py is the expected one
    using:
    - an end-to-end flow;
    - checked in files.
    """

    def _helper(self) -> None:
        # Set up.
        in_file = os.path.join(self.get_input_dir(), "input1.txt")
        out_file = os.path.join(self.get_scratch_space(), "output.txt")
        # Run.
        act = _run_preprocess(in_file, out_file)
        # Check.
        self.check_string(act)

    def test1(self):
        self._helper()

    def test2(self):
        self._helper()


@pytest.mark.skip
class Test_preprocess2(ut.TestCase):
    """
    Check that the output of preprocess_md_for_pandoc.py is the expected one
    using:
    - an end-to-end flow;
    - small snippets of text.
    """

    def _helper(self, txt_in: str, exp: str) -> None:
        # Set up.
        in_file = os.path.join(self.get_scratch_space(), "input1.txt")
        io_.to_file(in_file, txt_in)
        dbg.dassert_exists(in_file)
        _LOG.debug("Written %s", in_file)
        #
        out_file = os.path.join(self.get_scratch_space(), "output.txt")
        # Run.
        act = _run_preprocess(in_file, out_file)
        # Check.
        self.assert_equal(act, exp)

    def test1(self):
        txt_in = """
# ##########
# Python: nested functions
# ##########
- Functions can be declared in the body of another function
- E.g., to hide utility functions in the scope of the function that uses them
    ```python
    def print_integers(values):

        def _is_integer(value):
            try:
                return value == int(value)
            except:
                return False

        for v in values:
            if _is_integer(v):
                print(v)
    ```
"""
        txt_out = """
# Python: nested functions
- Functions can be declared in the body of another function
- E.g., to hide utility functions in the scope of the function that uses them
```python
    def print_integers(values):

        def _is_integer(value):
            try:
                return value == int(value)
            except:
                return False

        for v in values:
            if _is_integer(v):
                print(v)
```
"""
        self._helper(txt_in, txt_out)
