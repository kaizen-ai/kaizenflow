import logging

import pytest

import core.config as cconfig
import dataflow.notebooks.tutorial_dataflow as dntd
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestRunTutorialDataflowNotebook(hunitest.TestCase):
    @pytest.mark.slow
    def test_run_notebook(self) -> None:
        """
        Test if the notebook run end-to-end with no errors.
        """
        notebook_path = "dataflow/notebooks/tutorial_dataflow.ipynb"
        config_func_path = (
            "dataflow.notebooks.test.test_tutorial_dataflow.build_config_list()"
        )
        dst_dir = "dataflow/notebooks/test/test_results"
        # Build the command with parameters.
        cmd = f"""run_notebook.py \
            --notebook {notebook_path} \
            --config_builder "{config_func_path}" \
            --dst_dir {dst_dir} \
            --num_threads 2
        """
        # Run the notebook.
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
