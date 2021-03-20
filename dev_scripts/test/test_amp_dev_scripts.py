import logging
import os
from typing import List, Tuple

import pytest

import dev_scripts.url as url
import helpers.env as env
import helpers.git as git
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
