import logging
import os
from typing import List

import pytest

import dev_scripts.url as url
import helpers.conda as hco
import helpers.dbg as dbg
import helpers.env as env
import helpers.git as git
import helpers.io_ as io_
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################
# url.py
# #############################################################################


class Test_url_py1(ut.TestCase):
    def test_get_file_name1(self) -> None:
        url_tmp = (
            "http://localhost:10001/notebooks/oil/ST/"
            + "Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_file_name(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)

    def test_get_file_name2(self) -> None:
        url_tmp = (
            "https://github.com/ParticleDev/commodity_research/blob/"
            + "master/oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_file_name(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)

    def test_run1(self) -> None:
        exec_name = git.find_file_in_git_tree("url.py")
        cmd = (
            "%s " % exec_name + "http://localhost:9999/notebooks/research/"
            "Task51_experiment_with_sklearn_pipeline/"
            "Task51_experiment_with_sklearn_pipeline.ipynb"
        )
        si.system(cmd)


# #############################################################################


# TODO(gp): Move this to test to the proper dir `helpers/test/test.py`


class Test_env1(ut.TestCase):
    def test_get_system_signature1(self) -> None:
        _ = env.get_system_signature()


# #############################################################################
# _setenv.py
# #############################################################################

# TODO(gp): Call _setenv.py as a library to increase converage and get
#  annotations from pyannotate.


class Test_set_env_amp(ut.TestCase):
    def test_setenv_py1(self) -> None:
        """
        Find _setenv_amp.py executable and run it.
        """
        executable = git.find_file_in_git_tree(
            "_setenv_amp.py", super_module=False
        )
        executable = os.path.abspath(executable)
        _LOG.debug("executable=%s", executable)
        dbg.dassert_exists(executable)
        si.system(executable)

    # Since there are dependency from the user environment, we freeze a
    # particular run of _setenv_amp.py.
    @pytest.mark.skipif('si.get_user_name() != "saggese"')
    def test_setenv_py2(self):
        """
        Find _setenv_amp.py executable, run it, and freeze the output.
        """
        executable = git.find_file_in_git_tree(
            "_setenv_amp.py", super_module=False
        )
        executable = os.path.abspath(executable)
        _LOG.debug("executable=%s", executable)
        dbg.dassert_exists(executable)
        # Run _setup.py and get its output.
        _, txt = si.system_to_string(executable)
        # There is a difference between running the same test from different
        # repos, so we remove this line.
        # echo 'curr_path=$GIT_ROOT/amp' |     echo 'curr_path=$GIT_ROOT'
        txt = ut.filter_text("curr_path=", txt)
        self.check_string(txt)

    def test_setenv_sh1(self) -> None:
        """
        Execute setenv_amp.sh.
        """
        executable = git.find_file_in_git_tree(
            "setenv_amp.sh", super_module=False
        )
        executable = os.path.abspath(executable)
        _LOG.debug("executable=%s", executable)
        dbg.dassert_exists(executable)
        cmd = "source %s amp_develop" % executable
        si.system(cmd)


# #############################################################################
# jack*
# #############################################################################


class Test_jack1(ut.TestCase):

    # TODO(gp): Not clear why it's broken.
    @pytest.mark.skipif('si.get_user_name() == "jenkins"')
    def test_jack(self):
        cmd = 'jack "def dassert"'
        si.system(cmd)

    @pytest.mark.skipif('si.get_user_name() == "jenkins"')
    def test_jackpy(self):
        cmd = 'jackpy "def dassert"'
        si.system(cmd)

    def test_jackipynb(self) -> None:
        cmd = 'jackipynb "import"'
        si.system(cmd)

    def test_jackppy(self) -> None:
        cmd = 'jackipynb "import"'
        si.system(cmd)

    def test_jacktxt(self) -> None:
        cmd = 'jacktxt "python"'
        si.system(cmd)


# #############################################################################
# create_conda.py
# #############################################################################


class Test_install_create_conda_py1(ut.TestCase):
    def _run_create_conda(self, cmd_opts: List[str], cleanup: bool) -> None:
        """
        Run a create_conda command using custom options `cmd_opts`.

        :param cleanup: True if we want to cleanup the conda env instead of
            creating it.
        """
        exec_file = git.find_file_in_git_tree("create_conda.py")
        cmd = []
        cmd.append(exec_file)
        cmd.extend(cmd_opts)
        cmd.append("--delete_env_if_exists")
        cmd.append("-v DEBUG")
        # TODO(gp): Find a way to check the output looking at the packages.
        if cleanup:
            if ut.get_incremental_tests():
                # No clean up for manual inspection with:
                _LOG.warning("No clean up as per incremental test mode")
                return
            # Remove env.
            cmd.append("--skip_install_env")
            cmd.append("--skip_test_env")
        cmd_tmp = " ".join(cmd)
        si.system(cmd_tmp)

    def _helper(self, env_name: str, cmd_opts: List[str]) -> None:
        """
        Run create_conda with custom options `cmd_opts` and then remove the env.
        """
        self._run_create_conda(cmd_opts, cleanup=False)
        #
        cmd = "conda activate %s && conda info --envs" % env_name
        hco.conda_system(cmd, suppress_output=False)
        # Clean up the env.
        self._run_create_conda(cmd_opts, cleanup=True)

    def test_create_conda_test_install1(self) -> None:
        """
        Run create_conda with --test_install to exercise the script.
        """
        cmd_opts = [""]
        env_name = "test_install"
        cmd_opts.append(f"--env_name {env_name}")
        cmd_opts.append("--req_file dummy")
        cmd_opts.append("--test_install")
        self._helper(env_name, cmd_opts)

    @pytest.mark.slow
    def test_create_conda_yaml1(self):
        """
        Run create_conda.py with a single YAML file.
        """
        yaml = """
channels:
  - conda-forge
  - quantopian
dependencies:
  - pandas
  - pandas-datareader=0.8.0     # PartTask344.
  - pip
  - pip:
    #- ta                   # Technical analysis package.
    - trading-calendars
    """
        yaml_file = os.path.join(self.get_scratch_space(), "reqs.yaml")
        io_.to_file(yaml_file, yaml)
        #
        cmd_opts = []
        env_name = "test_create_conda_yaml1"
        cmd_opts.append(f"--env_name {env_name}")
        cmd_opts.append(f"--req_file {yaml_file}")
        self._helper(env_name, cmd_opts)

    @pytest.mark.slow
    def test_create_conda_yaml2(self):
        """
        Run create_conda.py with two YAML files.
        """
        yaml1 = """
channels:
  - conda-forge
  - quantopian
dependencies:
  - pandas
  - pandas-datareader=0.8.0     # PartTask344.
  - pip:
    #- ta                   # Technical analysis package.
    - trading-calendars """
        yaml_file1 = os.path.join(self.get_scratch_space(), "reqs1.yaml")
        io_.to_file(yaml_file1, yaml1)
        #
        yaml2 = """
channels:
  - conda-forge
  - quantopian
dependencies:
  - numpy"""
        yaml_file2 = os.path.join(self.get_scratch_space(), "reqs2.yaml")
        io_.to_file(yaml_file2, yaml2)
        #
        cmd_opts = []
        env_name = "test_create_conda_yaml2"
        cmd_opts.append(f"--env_name {env_name}")
        cmd_opts.append(f"--req_file {yaml_file1}")
        cmd_opts.append(f"--req_file {yaml_file2}")
        self._helper(env_name, cmd_opts)


# #############################################################################
# linter.py
# #############################################################################

import dev_scripts.linter as lntr

class Test_linter_py1(ut.TestCase):

    @staticmethod
    def _get_horrible_python_code1():
        txt = r"""
import python


if __name__ == "main":
    txt = "hello"
    m = re.search("\s", txt)
        """
        return txt

    def _write_input_file(self, txt):
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "input.py")
        file_name = os.path.abspath(file_name)
        io_.to_file(file_name, txt)
        return dir_name, file_name

    def _run_linter(self, file_name, linter_log, as_system_call):
        if as_system_call:
            cmd = []
            cmd.append(f"linter.py -f {file_name} --linter_log {linter_log}")
            cmd = " ".join(cmd)
            # We need to ignore the errors reported by the script, since it
            # represents how many lints were found.
            si.system(cmd, abort_on_error=False)
        else:
            parser = lntr._parser()
            args = parser.parse_args(["-f", file_name, "--linter_log",
                                      linter_log])
            lntr._main(args)
        # Read log.
        _LOG.debug("linter_log=%s", linter_log)
        txt = io_.from_file(linter_log, split=False)
        # Process log.
        output = []
        for l in txt.split("\n"):
            # Remove the line:
            #   cmd line='.../linter.py -f input.py --linter_log ./linter.log'
            if "cmd line=" in l:
                continue
            output.append(l)
        output = "\n".join(output)
        return output

    def _helper(self, txt, as_system_call):
        # Create file to lint.
        dir_name, file_name = self._write_input_file(txt)
        # Run.
        dir_name = self.get_scratch_space()
        linter_log = "linter.log"
        linter_log = os.path.abspath(os.path.join(dir_name, linter_log))
        output = self._run_linter(file_name, linter_log, as_system_call)
        return output

    def test_linter1(self) -> None:
        txt = self._get_horrible_python_code1()
        # Run.
        as_system_call = True
        output = self._helper(txt, as_system_call)
        # Check.
        self.check_string(output)

    def test_linter2(self) -> None:
        txt = self._get_horrible_python_code1()
        # Run.
        as_system_call = False
        output = self._helper(txt, as_system_call)
        # Check.
        self.check_string(output)

    # ##########################################################################

    def _helper_check_shebang(self, file_name, txt, is_executable):
        txt = txt.split("\n")
        msg = lntr._CustomPythonChecks._check_shebang(file_name, txt,
                                                      is_executable)
        self.check_string(msg)

    def test_check_shebang1(self):
        """
        Executable with wrong shebang: error.
        """
        file_name = "exec.py"
        txt = """#!/bin/bash
hello
world
"""
        is_executable = True
        self._helper_check_shebang(file_name, txt, is_executable)

    def test_check_shebang2(self):
        """
        Executable with the correct shebang: correct.
        """
        file_name = "exec.py"
        txt = """#!/usr/bin/env python
hello
world
"""
        is_executable = True
        self._helper_check_shebang(file_name, txt, is_executable)

    def test_check_shebang3(self):
        """
        Non executable with a shebang: error.
        """
        file_name = "exec.py"
        txt = """#!/usr/bin/env python
hello
world
"""
        is_executable = False
        self._helper_check_shebang(file_name, txt, is_executable)

    def test_check_shebang4(self):
        """
        Library without a shebang: correct.
        """
        file_name = "lib.py"
        txt = '''"""
Import as:

import _setenv_lib as selib
'''
        is_executable = False
        self._helper_check_shebang(file_name, txt, is_executable)

    # #########################

    def _helper_was_baptized(self, file_name, txt):
        txt = txt.split("\n")
        msg = lntr._CustomPythonChecks._was_baptized(file_name, txt)
        self.check_string(msg)

    def test_was_baptized1(self):
        file_name = "lib.py"
        txt = '''"""
Import as:

import _setenv_lib as selib
'''
        self._helper_was_baptized(file_name, txt)

    def test_was_baptized2(self):
        file_name = "lib.py"
        txt = '''"""
Import as:

import _setenv_lib as selib
'''
        self._helper_was_baptized(file_name, txt)

    # #########################

    def _helper_check_text(self, file_name, txt):
        txt = txt.split("\n")
        output = lntr._CustomPythonChecks._check_text(file_name, txt)
        msg = "\n".join(output)
        self.check_string(msg)

    def test_check_text1(self):
        file_name = "lib.py"
        txt = "from pandas import DataFrame"
        self._helper_check_text(file_name, txt)

    def test_check_text2(self):
        file_name = "lib.py"
        txt = "from typing import List"
        self._helper_check_text(file_name, txt)

    def test_check_text3(self):
        file_name = "lib.py"
        txt = "import pandas as very_long_name"
        self._helper_check_text(file_name, txt)
