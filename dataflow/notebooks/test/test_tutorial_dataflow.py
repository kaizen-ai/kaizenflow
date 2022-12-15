import logging

import pytest
import helpers.hunit_test as hunitest
import helpers.hsystem as hsystem


_LOG = logging.getLogger(__name__)

class TestRunTutorialDataflowNotebook(hunitest.TestCase):
    @pytest.mark.slow
    def test_run_notebook(self):
        """
        """
        cmd = """run_notebook.py \
            --notebook dataflow/notebooks/tutorial_dataflow.ipynb \
            --config_builder "dataflow.notebooks.tutorial_dataflow.get_gallery_dataflow_example_config()" \
            --dst_dir dataflow/notebooks/test/test_results \
            --num_threads 2
        """
        hsystem.system(cmd)
        