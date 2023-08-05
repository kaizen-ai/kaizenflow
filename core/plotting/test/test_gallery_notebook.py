import logging
import os

import pytest

import core.config as cconfig
import dev_scripts.notebooks as dsnrn
import helpers.hgit as hgit

_LOG = logging.getLogger(__name__)


def build_config_list() -> cconfig.ConfigList:
    """
    Simple config builder used by the test.
    """
    # We want to execute the notebook as it is, but config is needed
    # so we pass an empty one.
    config = {"figsize": (20, 10)}
    config = cconfig.Config()
    cconfig.ConfigList([config])
    return config


@pytest.mark.superslow("~40 sec.")
class TestGalleryNotebook(dsnrn.Test_Run_Notebook_TestCase):
    def test_run_notebook(self) -> None:
        """
        Test if gallery notebook runs end-to-end with no errors.
        """
        amp_path = hgit.get_amp_abs_path()
        notebook_path = os.path.join(
            amp_path, "core/plotting/notebooks/gallery_notebook.ipynb"
        )
        config_func_path = os.path.join(
            amp_path,
            "core.plotting.notebooks.test.test_gallery_notebook.build_config_list()",
        )
        self._test_run_notebook(notebook_path, config_func_path)
