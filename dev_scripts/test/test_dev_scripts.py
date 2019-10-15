import logging
import os

import pytest

import dev_scripts.url as url
import helpers.dbg as dbg
import helpers.env as env
import helpers.git as git
import helpers.io_ as io_
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################


class Test_url_py1(ut.TestCase):
    # TODO(gp): -> test_get_file_name
    def test1(self):
        url_tmp = (
            "http://localhost:10001/notebooks/oil/ST/"
            + "Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_file_name(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)

    def test2(self):
        url_tmp = (
            "https://github.com/ParticleDev/commodity_research/blob/"
            + "master/oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_file_name(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)

    def test_run1(self):
        # TODO(gp): Use git.find_file_in_git_tree()
        cmd = (
            "url.py "
            "http://localhost:9999/notebooks/research/"
            "Task51_experiment_with_sklearn_pipeline/"
            "Task51_experiment_with_sklearn_pipeline.ipynb"
        )
        si.system(cmd)


# #############################################################################


class Test_env1(ut.TestCase):
    def test_get_system_signature1(self):
        _ = env.get_system_signature()


# #############################################################################


class Test_set_env1(ut.TestCase):
    def test_setenv_py1(self):
        """
        Find _setenv.py executable and run it.
        """
        # TODO(gp): Use git.find_file_in_git_tree()
        cmd = "find . -name _setenv.py"
        _, txt = si.system_to_string(cmd)
        _LOG.debug("txt=%s", txt)
        #
        executable = os.path.abspath(txt)
        _LOG.debug("executable=%s", executable)
        dbg.dassert_exists(executable)
        si.system(executable)

    def test_setenv_sh1(self):
        """
        Execute setenv.sh.
        """
        cmd = "source dev_scripts/setenv.sh"
        si.system(cmd)


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

    def test_jackipynb(self):
        cmd = 'jackipynb "import"'
        si.system(cmd)

    def test_jackppy(self):
        cmd = 'jackipynb "import"'
        si.system(cmd)

    def test_jacktxt(self):
        cmd = 'jacktxt "python"'
        si.system(cmd)


# #############################################################################


class Test_install_create_conda_py1(ut.TestCase):
    def test_create_conda1(self):
        """
        Run create_conda with --test_install to exercise the script.
        """
        exec_file = git.find_file_in_git_tree("create_conda.py")
        cmd = f"{exec_file} --test_install --delete_env_if_exists -v DEBUG"
        si.system(cmd)
        # Remove env.
        cmd = cmd + "--skip_install_env --skip_test_env"
        si.system(cmd)

    @pytest.mark.slow
    def test_create_conda_yaml1(self):
        """
        Run create_conda.py with a single yaml file.
        """
        yaml = """
name: test_yaml
channels:
  - conda-forge
  - quantopian
dependencies:
  - pandas
  - pandas-datareader=0.8.0     # PartTask344.
  - pip:
    #- ta                   # Technical analysis package.
    - trading-calendars
    """
        yaml_file = os.path.join(self.get_scratch_space(), "reqs.yaml")
        io_.to_file(yaml_file, yaml)
        exec_file = git.find_file_in_git_tree("create_conda.py")
        env_name = "test_create_conda_yaml1"
        cmd = (
            f"{exec_file} --env_name {env_name} --req_file {yaml_file} "
            "--delete_env_if_exists -v DEBUG"
        )
        si.system(cmd)
        # TODO(gp): Find a way to check the output looking at the packages.
        if ut.get_incremental_tests():
            # No clean up for manual inspection.
            _LOG.warning("No clean up as per incremental test mode")
        else:
            # Remove env.
            cmd = cmd + " --skip_install_env --skip_test_env"
            si.system(cmd)

    @pytest.mark.slow
    def test_create_conda_yaml2(self):
        """
        Run create_conda.py with a two yaml files.
        """
        yaml1 = """
name: test_yaml
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
name: test_yaml
channels:
  - conda-forge
  - quantopian
dependencies:
  - numpy"""
        yaml_file2 = os.path.join(self.get_scratch_space(), "reqs2.yaml")
        io_.to_file(yaml_file1, yaml2)
        #
        exec_file = git.find_file_in_git_tree("create_conda.py")
        env_name = "test_create_conda_yaml2"
        cmd = (
            f"{exec_file} --env_name {env_name} --req_file {yaml_file1} "
            f"--req_file {yaml_file2} --delete_env_if_exists -v DEBUG"
        )
        si.system(cmd)
        # TODO(gp): Find a way to check the output looking at the packages.
        if ut.get_incremental_tests():
            # No clean up for manual inspection.
            _LOG.warning("No clean up as per incremental test mode")
        else:
            # Remove env.
            cmd = cmd + " --skip_install_env --skip_test_env"
            si.system(cmd)


# #############################################################################


class Test_linter_py1(ut.TestCase):
    def test_linter1(self):
        horrible_python_code = r"""
import python


if __name__ == "main":
    txt = "hello"
    m = re.search("\s", txt)
"""
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "input.py")
        file_name = os.path.abspath(file_name)
        io_.to_file(file_name, horrible_python_code)
        #
        linter_log = "./linter.log"
        # We run in the target dir so we have only relative paths, and we can
        # do a check of the output.
        base_name = os.path.basename(file_name)
        cmd = (
            f"cd {dir_name} && linter.py -f {base_name} --linter_log "
            f"{linter_log}"
        )
        si.system(cmd, abort_on_error=False)
        # Read log.
        linter_log = os.path.abspath(os.path.join(dir_name, linter_log))
        txt = io_.from_file(linter_log, split=False)
        output = []
        for l in txt.split("\n"):
            # Remove the line:
            #   cmd line='.../linter.py -f input.py --linter_log ./linter.log'
            if "cmd line=" in l:
                continue
            output.append(l)
        output = "\n".join(output)
        # Check.
        self.check_string(output)
