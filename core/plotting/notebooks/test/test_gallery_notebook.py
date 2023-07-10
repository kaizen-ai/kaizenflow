import logging

import pytest

import core.config as cconfig
import dev_scripts.notebooks as dsnrn

_LOG = logging.getLogger(__name__)

@pytest.mark.superslow("~40 sec.")
class TestGalleryNotebook(dsnrn.Test_Run_Notebook_TestCase):
    def test_run_notebook(self) -> None:
        """
        Test if gallery notebook runs end-to-end with no errors.
        """
        notebook_path = "core/plotting/notebooks/gallery_notebook.ipynb"
        config_func_path = "core.plotting.notebooks.test.test_gallery_notebook.build_config_list()"
        # Run the notebook.
        self._test_run_notebook(notebook_path, config_func_path)


def build_config_list() -> cconfig.ConfigList:
    """
    Simple config builder for the test.
    """
    # We want to execute the notebook as it is, but config builder needs
    # a config from the caller, which we ignore for now
    config = {}
    config = cconfig.Config()
    config_list = cconfig.ConfigList([config])
    return config_list


