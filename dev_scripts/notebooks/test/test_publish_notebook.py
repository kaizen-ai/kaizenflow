import logging
import os
from typing import Any, Dict, List, Tuple

import pytest

import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

# To avoid linter removing this.
_ = dbg, hprint, hsinte, pytest, Any, Dict, List, Tuple


_LOG = logging.getLogger(__name__)


class Test_publish_notebook1(hut.TestCase):
    def test_publish_local_notebook1(self) -> None:
        amp_dir = git.get_amp_abs_path()
        file_name = os.path.join(
            amp_dir,
            "core/dataflow_model/notebooks/Master_pipeline_runner.ipynb")
        cmd = []
        cmd.append("publish_notebook.py")
        cmd.append("--action publish")
        cmd.append(f"--file {file_name}")
        dst_dir = self.get_scratch_space()
        cmd.append(f"--publish_notebook_dir {dst_dir}")
        cmd = " ".join(cmd)
        hsinte.system(cmd)
