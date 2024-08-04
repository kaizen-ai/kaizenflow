import os
import py_compile

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hunit_test as hunitest


class TestDagCompildation(hunitest.TestCase):
    def helper(self, dags_dir: str) -> None:
        # Validate param.
        amp_path = hgit.get_amp_abs_path()
        amp_dags_dir = os.path.join(amp_path, dags_dir)
        hdbg.dassert_dir_exists(amp_dags_dir)
        # Fetch files from path.
        pattern = "*.py"
        only_files = False
        use_relative_paths = False
        python_files = hio.listdir(
            amp_dags_dir, pattern, only_files, use_relative_paths
        )
        # Run.
        for filename in python_files:
            py_compile.compile(filename, doraise=True)

    def test1(self) -> None:
        """
        Check if all datapull dag files are compiled successfully.
        """
        datapull_dags_dir = "im_v2/airflow/dags/datapull/"
        self.helper(datapull_dags_dir)

    def test2(self) -> None:
        """
        Check if all trading dag files are compiled successfully.
        """
        trading_dags_dir = "im_v2/airflow/dags/trading/"
        self.helper(trading_dags_dir)
