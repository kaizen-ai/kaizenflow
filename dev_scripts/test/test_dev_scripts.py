import logging
import os

import dev_scripts.url as url
import helpers.dbg as dbg
import helpers.env as env
import helpers.unit_test as ut
import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)


# #############################################################################


class Test_url_py1(ut.TestCase):
    def test1(self):
        url_tmp = (
            "http://localhost:10001/notebooks/oil/ST/"
            "Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_root(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)

    def test2(self):
        url_tmp = (
            "https://github.com/ParticleDev/commodity_research/blob/"
            "master/oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        )
        act = url._get_root(url_tmp)
        exp = "oil/ST/Task229_Exploratory_analysis_of_ST_data_part1.ipynb"
        self.assertEqual(act, exp)


# #############################################################################


class Test_env1(ut.TestCase):
    def test_env1(self):
        _ = env.get_system_signature()


# #############################################################################


class Test_set_env1(ut.TestCase):
    def test_setenv_py1(self):
        cmd = "find . -name _setenv.py"
        _, txt = si.system_to_string(cmd)
        _LOG.debug("txt=%s", txt)
        exec = os.path.abspath(txt)
        _LOG.debug("exec=%s", exec)
        dbg.dassert_exists(exec)
        si.system(exec)

    def test_setenv_sh1(self):
        cmd = "source dev_scripts/setenv.sh"
        si.system(cmd)
