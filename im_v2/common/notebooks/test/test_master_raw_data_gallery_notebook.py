import pytest

import core.config as cconfig
import dev_scripts.notebooks as dsnrn


def build_config() -> cconfig.ConfigList:
    """
    Simple config builder for the test.
    """
    # We want to execute the notebook as it is, but config builder needs
    # a config from the caller, which we ignore for now
    config = {}
    config = cconfig.Config()
    config_list = cconfig.ConfigList([config])
    return config_list


@pytest.mark.superslow("~40 sec.")
class Test_run_master_raw_data_gallery_notebook(dsnrn.Test_Run_Notebook_TestCase):
    def test1(self) -> None:
        notebook_path = "im_v2/common/notebooks/Master_raw_data_gallery.ipynb"
        config_builder = "im_v2.common.notebooks.test.test_master_raw_data_gallery_notebook.build_config()"
        self._test_run_notebook(notebook_path, config_builder)
