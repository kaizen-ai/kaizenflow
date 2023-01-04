import logging

import pytest

import core.config as cconfig
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import research_amp.cc.algotrading as ramccalg

_LOG = logging.getLogger(__name__)


class TestCmTask3352Notebook(hunitest.TestCase):
    @pytest.mark.slow
    def test_run_notebook(self) -> None:
        """
        Test if the notebook runs end-to-end with no errors.
        """
        notebook_path = "research_amp/cc/notebooks/CMTask3352_pegged_at_mid_algo_simulation.ipynb"
        config_func_path = (
            "research_amp.cc.notebooks.test.test_algo_notebook.build_config_list()"
        )
        dst_dir = "cc/notebooks/test/outcomes"
        # Build the command with parameters.
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