import glob
import logging
import os
from typing import List

import pytest

import documentation.scripts.convert_txt_to_pandoc as dscttp
import documentation.scripts.lint_txt as dslt
import documentation.scripts.render_md as rmd
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
@pytest.mark.skipif(
    'not (si.get_user_name() == "saggese" and si.get_os_name() == "Darwin")'
)
class Test_pandoc1(ut.TestCase):
    def _helper(self, in_file: str, action: str) -> str:
        exec_path = git.find_file_in_git_tree("pandoc.py")
        dbg.dassert_exists(exec_path)
        #
        tmp_dir = self.get_scratch_space()
        out_file = os.path.join(tmp_dir, "output.pdf")
        cmd = []
        cmd.append(exec_path)
        cmd.append("--type %s" % action)
        cmd.append("--tmp_dir %s" % tmp_dir)
        cmd.append("--input %s" % in_file)
        cmd.append("--output %s" % out_file)
        cmd.append("--action convert_txt_to_pandoc")
        cmd.append("--action run_pandoc")
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

    def test1(self) -> None:
        """
        Convert one txt file to PDF and check that the .tex file is as expected.
        """
        file_name = "code_style.txt.test"
        file_name = os.path.join(self.get_input_dir(), file_name)
        file_name = os.path.abspath(file_name)
        #
        act = self._helper(file_name, "pdf")
        self.check_string(act)

    # TODO(gp): This seems flakey.
    def test2(self) -> None:
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

    def test_all_notes(self) -> None:
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
    return act  # type: ignore


# TODO(gp): -> Test_convert_txt_to_pandoc*
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

    def test1(self) -> None:
        self._helper()

    def test2(self) -> None:
        self._helper()


class Test_preprocess2(ut.TestCase):
    """
    Check that the output of convert_txt_to_pandoc.py is the expected one
    calling the library function directly.
    """

    def _helper_process_question(
        self, txt_in: str, do_continue_exp: bool, exp: str
    ) -> None:
        do_continue, act = dscttp._process_question(txt_in)
        self.assertEqual(do_continue, do_continue_exp)
        self.assert_equal(act, exp)

    def test_process_question1(self) -> None:
        txt_in = "* Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question2(self) -> None:
        txt_in = "** Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question3(self) -> None:
        txt_in = "*: Hope is not a strategy"
        do_continue_exp = True
        exp = "- **Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question4(self) -> None:
        txt_in = "- Systems don't run themselves, they need to be run"
        do_continue_exp = False
        exp = txt_in
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question5(self) -> None:
        space = "   "
        txt_in = "*" + space + "Hope is not a strategy"
        do_continue_exp = True
        exp = "-" + space + "**Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    def test_process_question6(self) -> None:
        space = "   "
        txt_in = "**" + space + "Hope is not a strategy"
        do_continue_exp = True
        exp = "-" + " " * len(space) + "**Hope is not a strategy**"
        self._helper_process_question(txt_in, do_continue_exp, exp)

    # #########################################################################

    def _helper_transform(self, txt_in: str, exp: str) -> None:
        act_as_arr = dscttp._transform(txt_in.split("\n"))
        act = "\n".join(act_as_arr)
        self.assert_equal(act, exp)

    def test_transform1(self) -> None:
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
    def _helper_preprocess(self, txt: str, exp: str) -> None:
        act = dslt._preprocess(txt)
        self.assert_equal(act, exp)

    def test_preprocess1(self) -> None:
        txt = r"""$$E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)$$"""
        exp = r"""$$
E_{in} = \frac{1}{N} \sum_i e(h(\vx_i), y_i)
$$"""
        self._helper_preprocess(txt, exp)

    def test_preprocess2(self) -> None:
        txt = r"""$$E_{in}(\vw) = \frac{1}{N} \sum_i \big(
-y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
\big)$$"""
        exp = r"""$$
E_{in}(\vw) = \frac{1}{N} \sum_i \big(
-y_i \log(\Pr(h(\vx) = 1|\vx)) - (1 - y_i) \log(1 - \Pr(h(\vx)=1|\vx))
\big)
$$"""
        self._helper_preprocess(txt, exp)

    @staticmethod
    def _get_text1() -> str:
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

    def test_preprocess3(self) -> None:
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
        self._helper_preprocess(txt, exp)

    def test_preprocess4(self) -> None:
        txt = r"""# #########################
# test
# #############################################################################"""
        exp = r"""# test"""
        self._helper_preprocess(txt, exp)

    def test_preprocess5(self) -> None:
        txt = r"""## ////////////////
# test
# ////////////////"""
        exp = r"""# test"""
        self._helper_preprocess(txt, exp)

    # #########################################################################

    def _helper_process(self, txt, exp, file_name) -> None:
        file_name = os.path.join(self.get_scratch_space(), file_name)
        act = dslt._process(txt, file_name)
        if exp:
            self.assert_equal(act, exp)
        return act

    def test_process1(self) -> None:
        txt = self._get_text1()
        exp = None
        file_name = "test.txt"
        act = self._helper_process(txt, exp, file_name)
        self.check_string(act)

    def test_process2(self) -> None:
        """
        Run the text linter on a txt file.
        """
        txt = r"""
*  Good time management

1. choose the right tasks
    -   avoid non-essential tasks
"""
        exp = r"""* Good time management

1. Choose the right tasks
   - Avoid non-essential tasks
"""
        file_name = "test.txt"
        self._helper_process(txt, exp, file_name)

    def test_process3(self) -> None:
        """
        Run the text linter on a md file.
        """
        txt = r"""
# Good
- Good time management
  1. choose the right tasks
    - Avoid non-essential tasks

## Bad
-  Hello
    - World
"""
        exp = r"""<!--ts-->
   * [Good](#good)
      * [Bad](#bad)



<!--te-->
# Good

- Good time management
  1. Choose the right tasks
  - Avoid non-essential tasks

## Bad

- Hello
  - World
"""
        file_name = "test.md"
        self._helper_process(txt, exp, file_name)

    def test_process4(self) -> None:
        """
        Check that no replacement happens inside a ``` block.
        """
        txt = r"""<!--ts-->
<!--te-->
- Good
- Hello
```test
- hello
    - world
1) oh no!
```
"""
        exp = r"""<!--ts-->
<!--te-->
- Good
- Hello
```test
- hello
    - world
1) oh no!
```
"""
        file_name = "test.md"
        act = self._helper_process(txt, None, file_name)
        act = prnt.remove_empty_lines(act)
        self.assert_equal(act, exp)

    @staticmethod
    def _get_text_problematic_for_prettier1() -> None:
        txt = r"""
* Python formatting
- Python has several built-in ways of formatting strings
  1) `%` format operator
  2) `format` and `str.format`


* `%` format operator
- Text template as a format string
  - Values to insert are provided as a value or a `tuple`
"""
        return txt

    def test_process_prettier_bug1(self) -> None:
        """
        For some reason prettier replaces - with * when there are 2 empty lines.
        """
        txt = self._get_text_problematic_for_prettier1()
        exp = r"""- Python formatting

* Python has several built-in ways of formatting strings
  1. `%` format operator
  2. `format` and `str.format`

- `%` format operator

* Text template as a format string
  - Values to insert are provided as a value or a `tuple`
"""
        act = dslt._prettier(txt)
        self.assert_equal(act, exp)

    def test_process5(self) -> None:
        """
        Run the text linter on a txt file.
        """
        txt = self._get_text_problematic_for_prettier1()
        exp = r"""* Python formatting
- Python has several built-in ways of formatting strings

  1. `%` format operator
  2. `format` and `str.format`

* `%` format operator
- Text template as a format string
  - Values to insert are provided as a value or a `tuple`
"""
        file_name = "test.txt"
        self._helper_process(txt, exp, file_name)

    def test_process6(self) -> None:
        """
        Run the text linter on a txt file.
        """
        txt = r"""
* `str.format`
- Python 3 allows to format multiple values, e.g.,
   ```python
   key = 'my_var'
   value = 1.234
   ```
"""
        exp = r"""* `str.format`
- Python 3 allows to format multiple values, e.g.,
  ```python
  key = 'my_var'
  value = 1.234
  ```
"""
        file_name = "test.txt"
        self._helper_process(txt, exp, file_name)


##################################
# render_md.py
##################################
class Test_render_md1(ut.TestCase):
    def test_uml_file_names1(self) -> None:
        """
        Check output dir and file names correctness for absolute 
        destination path.
        """
        dest_file = "/a/b/c/d/e.md"
        idx = 8
        extension = 'png'
        pathes = rmd._uml_file_names(dest_file, idx, extension)
        self.check_string("\n".join(pathes))

    def test_render_command1(self) -> None:
        """
        Check correctness of the command to render.
        """
        uml_file = "/a/b/c.puml"
        dest = "/d/e/f"
        extension = 'png'
        cmd = rmd._render_command(uml_file, dest, extension)
        self.check_string(cmd)

    def test_render_command2(self) -> None:
        """
        Check assertion if extension is unknown when render command is building.
        """
        uml_file = "/a/b/c.puml"
        dest = "/d/e/f"
        extension = 'bmp'
        with self.assertRaises(AssertionError) as exception:
            rmd._render_command(uml_file, dest, extension)
            self.assertIn('bmp', exception.msg)

    def test_render_plantuml1(self) -> None:
        """
        Check correctness of rendering just UML text.
        """
        in_text = [
            "```plantuml",
            "Alice --> Bob",
            "```",
        ]
        self._check_str_after_render(in_text)

    def test_render_plantuml2(self) -> None:
        """
        Check correctness of rendering UML with another text.
        """
        in_text = [
            "A",
            "```plantuml",
            "Alice --> Bob",
            "```",
            "B",
        ]
        self._check_str_after_render(in_text)

    def test_render_plantuml3(self) -> None:
        """
        Check correctness of rendering text without UML.
        """
        in_text = [
            "A",
            "```bash",
            "Alice --> Bob",
            "```",
            "B",
        ]
        self._check_str_after_render(in_text)

    def test_render_plantuml4(self) -> None:
        """
        Check correctness of rendering UML text already pre-formatted.
        """
        in_text = [
            "```plantuml",
            "@startuml",
            "Alice --> Bob",
            "@enduml",
            "```",
        ]
        self._check_str_after_render(in_text)

    def _check_str_after_render(self, in_text: List[str]) -> None:
        out_file = os.path.join(self.get_scratch_space(), 'out.md')
        extension = "png"
        out_text = rmd._render_plantuml(in_text, out_file, extension, dry_run=True)
        self.check_string("\n".join(out_text))
