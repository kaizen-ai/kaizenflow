import glob
import logging
import os

import pytest

import documentation.scripts.convert_txt_to_pandoc as dscttp
import documentation.scripts.lint_txt as dslt
import helpers.dbg as dbg
import helpers.git as git
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################
# pandoc.py
# #############################################################################


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
        act = io_.from_file(out_file)
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


# #############################################################################
# convert_txt_to_pandoc.py
# #############################################################################


def _run_preprocess(in_file: str, out_file: str) -> str:
    """
    Execute the end-to-end flow for convert_txt_to_pandoc.py returning
    the output as string.
    """
    exec_path = git.find_file_in_git_tree("convert_txt_to_pandoc.py")
    dbg.dassert_exists(exec_path)
    #
    dbg.dassert_exists(in_file)
    #
    cmd = []
    cmd.append(exec_path)
    cmd.append("--input %s" % in_file)
    cmd.append("--output %s" % out_file)
    cmd_as_str = " ".join(cmd)
    si.system(cmd_as_str)
    # Check.
    act = io_.from_file(out_file)
    return act


class Test_preprocess1(ut.TestCase):
    """
    Check that the output of convert_txt_to_pandoc.py is the expected one
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


class Test_preprocess2(ut.TestCase):
    """
    Check that the output of convert_txt_to_pandoc.py is the expected one
    calling the library function directly.
    """

    def _helper_process_question(
        self, txt_in: str, do_continue_exp: bool, exp: str
    ):
        do_continue, act = dscttp._process_question(txt_in)
        self.assertEqual(do_continue, do_continue_exp)
        self.assert_equal(act, exp)

    def test_process_question1(self):
        txt_in = "* Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question2(self):
        txt_in = "** Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question3(self):
        txt_in = "*: Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question4(self):
        txt_in = "- Systems don't run themselves, they need to be run"
        do_continue_exp = False
        exp = txt_in
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question5(self):
        space = "   "
        txt_in = "*" + space + "Hope is not a strategy"
        do_continue_exp = True
        exp = "-" + space + "**Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question6(self):
        space = "   "
        txt_in = "**" + space + "Hope is not a strategy"
        do_continue_exp = True
        exp = "-" + " " * len(space) + "**Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    # #########################################################################

    def _helper_transform(self, txt_in: str, exp: str):
        act_as_arr = dscttp._transform(txt_in.split("\n"))
        act = "\n".join(act_as_arr)
        self.assert_equal(act, exp)

    def test_transform1(self):
        txt_in = """
# #############################################################################
# Python: nested functions
# #############################################################################
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
        exp = """
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
        self._helper_transform(txt_in, exp)


# #############################################################################
# lint_txt.py
# #############################################################################


class Test_lint_txt1(ut.TestCase):
    def test_preprocess1(self):
        txt = r"""$$E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)$$"""
        act = dslt._preprocess(txt)
        exp = r"""$$
E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)
$$"""
        self.assert_equal(act, exp)

    def test_preprocess2(self):
        txt = r"""$$E_{in}(\vw) = \frac{1}{N} \sum_i \big(
-y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
\big)$$"""
        exp = r"""$$
E_{in}(\vw) = \frac{1}{N} \sum_i \big(
-y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
\big)
$$"""
        act = dslt._preprocess(txt)
        self.assert_equal(act, exp)

    @staticmethod
    def _get_text1():
        txt = r"""* Gradient descent for logistic regression
- The typical implementations of gradient descent (basic or advanced) need two
  inputs:
    - The cost function $E_{in}(\vw)$ (to monitor convergence)
    - The gradient of the cost function
      $\frac{\partial E}{w_j} \text{ for all } j$ (to optimize)
- The cost function is:
    $$E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)$$

- In case of general probabilistic model $h(\vx)$ in \{0, 1\}):
    $$
    E_{in}(\vw) = \frac{1}{N} \sum_i \big(
    -y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
    \big)
    $$

- In case of logistic regression in \{+1, -1\}:
    $$E_{in}(\vw) = \frac{1}{N} \sum_i \log(1 + \exp(-y_i \vw^T \vx_i))$$

- It can be proven that the function $E_{in}(\vw)$ to minimize is convex in
  $\vw$ (sum of exponentials and flipped exponentials is convex and log is
  monotone)"""
        return txt

    def test_preprocess3(self):
        txt = self._get_text1()
        exp = r"""- STARGradient descent for logistic regression
- The typical implementations of gradient descent (basic or advanced) need two
  inputs:
    - The cost function $E_{in}(\vw)$ (to monitor convergence)
    - The gradient of the cost function
      $\frac{\partial E}{w_j} \text{ for all } j$ (to optimize)
- The cost function is:
    $$
    E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)
    $$

- In case of general probabilistic model $h(\vx)$ in \{0, 1\}):
    $$
    E_{in}(\vw) = \frac{1}{N} \sum_i \big(
    -y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
    \big)
    $$

- In case of logistic regression in \{+1, -1\}:
    $$
    E_{in}(\vw) = \frac{1}{N} \sum_i \log(1 + \exp(-y_i \vw^T \vx_i))
    $$

- It can be proven that the function $E_{in}(\vw)$ to minimize is convex in
  $\vw$ (sum of exponentials and flipped exponentials is convex and log is
  monotone)"""
        act = dslt._preprocess(txt)
        self.assert_equal(act, exp)

    def test_preprocess4(self):
        txt = r"""# #########################
# test
# ###############"""
        act = dslt._preprocess(txt)
        exp = r"""# test"""
        self.assert_equal(act, exp)

    def test_preprocess5(self):
        txt = r"""## ////////////////
# test
# ////////////////"""
        act = dslt._preprocess(txt)
        exp = r"""# test"""
        self.assert_equal(act, exp)

    def test_process1(self):
        txt = self._get_text1()
        file_name = os.path.join(self.get_scratch_space(), "test.txt")
        act = dslt._process(txt, file_name)
        self.check_string(act)
