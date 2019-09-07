import logging

import dev_scripts.url as url
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestUrl1(ut.TestCase):
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
