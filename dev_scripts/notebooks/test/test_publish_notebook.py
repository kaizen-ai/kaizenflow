import logging
import os

import helpers.git as git
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_publish_notebook1(hut.TestCase):
    def test_publish_local_notebook1(self) -> None:
        """
        Publish locally a notebook as HTML.
        """
        amp_dir = git.get_amp_abs_path()
        file_name = os.path.join(
            amp_dir, "core/dataflow_model/notebooks/Master_pipeline_runner.ipynb"
        )
        cmd = []
        cmd.append("publish_notebook.py")
        cmd.append("--action publish_locally")
        cmd.append(f"--file {file_name}")
        dst_dir = self.get_scratch_space()
        cmd.append(f"--publish_notebook_dir {dst_dir}")
        cmd = " ".join(cmd)
        hsinte.system(cmd)


# TODO(gp): Test different actions

# TODO(gp): Add test for something like:
#  > publish_notebook.py \
#   --file http://127.0.0.1:2908/notebooks/notebooks/AmpTask40_Optimizer.ipynb \
#   --action post_on_s3 \
#   --s3_path s3://.../notebooks \
#   --aws_profile am

# TODO(gp): Fix this.
# http://127.0.0.1:5012/notebooks/amp/core/dataflow_model/notebooks/Master_model_analyzer.ipynb
# is not resolved correctly
# ################################################################################
# * Failed assertion *
# File '/app/amp/amp/core/dataflow_model/notebooks/Master_model_analyzer.ipynb' doesn't exist
# ################################################################################