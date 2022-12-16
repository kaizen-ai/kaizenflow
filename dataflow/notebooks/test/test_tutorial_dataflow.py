import logging

import pytest
import core.config as cconfig
import helpers.hunit_test as hunitest
import helpers.hsystem as hsystem
import dataflow.notebooks.tutorial_dataflow as dntd

_LOG = logging.getLogger(__name__)

class TestRunTutorialDataflowNotebook(hunitest.TestCase):
    @pytest.mark.slow
    def test_run_notebook(self) -> None:
        """
        Test if the notebook run end-to-end with no errors.
        """
        cmd = """run_notebook.py \
            --notebook dataflow/notebooks/tutorial_dataflow.ipynb \
            --config_builder "dataflow.notebooks.test.test_tutorial_dataflow.build_config_list()" \
            --dst_dir dataflow/notebooks/test/test_results \
            --num_threads 2
        """
        hsystem.system(cmd)


def build_config_list() -> cconfig.ConfigList:
    """
    Build the config to run the notebook.
    """
    config = dntd.get_gallery_dataflow_example_config()
    config["id"] = "0"
    configs = [config]
    config_list = cconfig.ConfigList(configs)
    return config_list


