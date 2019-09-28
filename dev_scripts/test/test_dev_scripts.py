import logging
import os

import pytest

import dev_scripts.url as url
import helpers.dbg as dbg
import helpers.env as env
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
