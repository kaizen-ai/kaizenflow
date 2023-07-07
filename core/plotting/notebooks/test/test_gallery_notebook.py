import logging

import pytest

import core.config as cconfig
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import research_amp.cc.algotrading as ramccalg

_LOG = logging.getLogger(__name__)


class TestSorrTask393Notebook(hunitest.TestCase):
    @pytest.mark.slow
    def test_run_notebook(self) -> None:
        """
        Test if gallery notebook runs end-to-end with no errors.
        """
        notebook_path = "core/plotting/notebooks/gallery_notebook.ipynb"
        config_func_path = "core.plotting.notebooks.test.test_gallery_notebook.build_config_list()"
        dst_dir = "core/plotting/notebooks/test/outcomes"
        #  Build the command with parameters.
        cmd = f"""run_notebook.py \
            --notebook {notebook_path} \
            --config_builder "{config_func_path}" \
            --dst_dir {dst_dir} \
            --num_threads 1
        """
        # Run the notebook.
        hsystem.system(cmd)


def build_config_list() -> cconfig.ConfigList:
    """
    Build the config to run the notebook.
    """
    config = ramccalg.get_default_config()
    config["id"] = "0"
    configs = [config]
    config_list = cconfig.ConfigList(configs)
    return config_list


