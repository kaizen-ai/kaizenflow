import os

import pytest

import core.config as cconfig
import dev_scripts.notebooks.run_notebook_test_case as dsnrnteca
import helpers.hgit as hgit


def build_config() -> cconfig.ConfigList:
    """
    Simple config builder for the test.
    """
    # We want to execute the notebook as it is, but config builder needs
    # a config from the caller, which we ignore for now.
    config = {}
    config = cconfig.Config()
    config_list = cconfig.ConfigList([config])
    return config_list


@pytest.mark.superslow("~60 sec.")
class Test_run_gallery_market_data_notebook(dsnrnteca.Test_Run_Notebook_TestCase):
    def test1(self) -> None:
        amp_dir = hgit.get_amp_abs_path()
        notebook_path = os.path.join(
            amp_dir, "market_data", "notebooks", "gallery_market_data.ipynb"
        )
        config_builder = (
            "market_data.test.test_gallery_market_data_notebook.build_config()"
        )
        self._test_run_notebook(notebook_path, config_builder)
