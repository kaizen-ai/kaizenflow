import logging
from typing import List

import core.config as cfg
import core.config_builders as cfgb
import helpers.system_interaction as si
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestRunNotebook(hut.TestCase):
    def test_main1(self) -> None:
        dst_dir = self.get_output_dir()
        cmd = (
            "/commodity_research/amp/dev_scripts/notebooks/run_notebook.py "
            f"--dst_dir {dst_dir} "
            "--notebook /commodity_research/amp/dev_scripts/notebooks/test_run_notebook.ipynb "
            "--function 'amp.dev_scripts.test.test_run_notebook.build_configs()' "
            "--abort_on_error False "
            "--num_threads 1"
        )
        si.system(cmd)

    def test_main2(self) -> None:
        dst_dir = self.get_output_dir()
        cmd = (
            "/commodity_research/amp/dev_scripts/notebooks/run_notebook.py "
            f"--dst_dir {dst_dir} "
            "--notebook /commodity_research/amp/dev_scripts/notebooks/test_run_notebook.ipynb "
            "--function 'amp.dev_scripts.test.test_run_notebook.build_configs()' "
            "--abort_on_error False "
            "--num_threads 3"
        )
        si.system(cmd)


def build_configs() -> List[cfg.Config]:
    config_template = cfg.Config()
    config_template["fail"] = None
    configs = cfgb.build_multiple_configs(
        config_template, {("fail",): (False, False, True, False, False)}
    )
    # Duplicate configs are not allowed, so we need to add identifiers for
    # them.
    for i, config in enumerate(configs):
        config["id"] = i
    return configs
