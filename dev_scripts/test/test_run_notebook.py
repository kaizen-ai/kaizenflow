import itertools
import logging
import os
from typing import Any, Dict, Iterable, List, Tuple, Generator

import pytest

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import unittest.mock as umock
from invoke import Context
import dev_scripts.lib_tasks_run_model_experiment_notebooks as run_experiment_notebooks
_LOG = logging.getLogger(__name__)


# #############################################################################


# These are fake config builders used for testing.


def _config_builder(fail: bool) -> cconfig.Config:
    """
    Simple config builder for the test.
    """
    config = {"fail": fail}
    config = cconfig.Config().from_dict(config)
    config_list = cconfig.ConfigList([config])
    return config_list


def _build_multiple_configs(
    template_config: cconfig.Config,
    params_variants: Dict[Tuple[str, ...], Iterable[Any]],
) -> List[cconfig.Config]:
    """
    Build configs from a template and the Cartesian product of given keys/vals.

    Create multiple `cconfig.Config` objects using the given config template and
    overwriting `None` or `cconfig.DUMMY` parameter specified through a parameter
    path and several possible elements:
        param_path: Tuple(str) -> param_values: Iterable[Any]
    A parameter path is represented by a tuple of nested names.

    Note that we create a config for each element of the Cartesian product of
    the values to be assigned.

    :param template_config: cconfig.Config object
    :param params_variants: {(param_name_in_the_config_path):
        [param_values]}, e.g. {('read_data', 'symbol'): ['CL', 'QM'],
                                ('resample', 'rule'): ['5T', '10T']}
    :return: a list of configs
    """
    # In the example from above:
    # ```
    # list(params_values) = [('CL', '5T'), ('CL', '10T'), ('QM', '5T'), ('QM', '10T')]
    # ```
    params_values = itertools.product(*params_variants.values())
    param_vars = list(
        dict(zip(params_variants.keys(), values)) for values in params_values
    )
    # In the example above:
    # ```
    # param_vars = [
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'CL', ('resample', 'rule'): '10T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '5T'},
    #    {('read_data', 'symbol'): 'QM', ('resample', 'rule'): '10T'},
    #  ]
    # ```
    param_configs = []
    for params in param_vars:
        # Create a config for the chosen parameter values.
        config_var = template_config.copy()
        for param_path, param_val in params.items():
            # Select the path for the parameter and set the parameter.
            conf_tmp = config_var
            for pp in param_path[:-1]:
                conf_tmp.check_params([pp])
                conf_tmp = conf_tmp[pp]
            conf_tmp.check_params([param_path[-1]])
            if not (
                conf_tmp[param_path[-1]] is None
                or conf_tmp[param_path[-1]] == cconfig.DUMMY
            ):
                raise ValueError(
                    "Trying to change a parameter that is not `None` or "
                    "`'cconfig.DUMMY'`. Parameter path is %s" % str(param_path)
                )
            conf_tmp[param_path[-1]] = param_val
        param_configs.append(config_var)
    return param_configs


def _build_config_list(values: List[bool]) -> cconfig.ConfigList:
    # Create config in `overwrite` mode to allow reassignment of values.
    update_mode = "overwrite"
    config_template = cconfig.Config(update_mode=update_mode)
    # TODO(gp): -> fail_param
    config_template["fail"] = None
    configs = _build_multiple_configs(config_template, {("fail",): values})
    # Duplicated configs are not allowed, so we need to add identifiers to make
    # each config unique.
    for i, config in enumerate(configs):
        config["id"] = str(i)
    config_list = cconfig.ConfigList(configs)
    return config_list


def build_config_list1() -> cconfig.ConfigList:
    """
    Build 2 configs that won't make the notebook to fail.
    """
    values = [False, False]
    config_list = _build_config_list(values)
    return config_list


def build_config_list2() -> cconfig.ConfigList:
    """
    Build 3 configs with one failing.
    """
    values = [False, False, True]
    config_list = _build_config_list(values)
    return config_list


def build_config_list3() -> cconfig.ConfigList:
    """
    Build 1 config that won't make the notebook to fail.
    """
    values = [False]
    config_list = _build_config_list(values)
    return config_list


# #############################################################################


def _compare_dir_signature(self: Any, dir_name: str, expected: str) -> None:
    """
    Compute the signature of dir `dir_name` to the expected value `expected`.
    """
    # Compute and compare the dir signature.
    actual = hunitest.get_dir_signature(
        dir_name, include_file_content=False, num_lines=None
    )
    # Remove references like:
    # $GIT_ROOT/core/dataflow/backtest/test/TestRunExperiment1.test3/tmp.scratch
    actual = actual.replace(dir_name, "$SCRATCH_SPACE")
    actual = hunitest.purify_txt_from_client(actual)
    # Remove lines like:
    # $GIT_ROOT/core/dataflow_model/.../log.20210705_100612.txt
    actual = hunitest.filter_text(r"^.*/log\.\S+\.txt$", actual)
    expected = hprint.dedent(expected)
    self.assert_equal(actual, expected, fuzzy_match=True)


def run_cmd_line(
    self: Any,
    cmd: List[str],
    cmd_opts: List[str],
    dst_dir: str,
    expected: str,
    expected_pass: bool,
) -> None:
    """
    Run run_config_list / run_notebook command line and check return code and
    output.
    """
    # Assemble the command line.
    cmd.extend(cmd_opts)
    cmd = " ".join(cmd)
    # Run command.
    abort_on_error = expected_pass
    _LOG.debug(
        "expected_pass=%s abort_on_error=%s", expected_pass, abort_on_error
    )
    _LOG.debug("cmd=%s", cmd)
    rc = hsystem.system(cmd, abort_on_error=abort_on_error, suppress_output=False)
    if expected_pass:
        self.assertEqual(rc, 0)
    else:
        self.assertNotEqual(rc, 0)
    _compare_dir_signature(self, dst_dir, expected)


def _get_files() -> Tuple[str, str]:
    amp_path = hgit.get_amp_abs_path()
    #
    exec_file = os.path.join(amp_path, "dev_scripts/notebooks/run_notebook.py")
    hdbg.dassert_file_exists(exec_file)
    # This notebook fails/succeeds depending on the return code stored inside
    # each config.
    notebook_file = os.path.join(
        amp_path, "dev_scripts/notebooks/test/simple_notebook.ipynb"
    )
    hdbg.dassert_file_exists(notebook_file)
    return exec_file, notebook_file


def _run_notebook_helper(
    self: Any, cmd_opts: List[str], exp_pass: bool, exp: str
) -> None:
    # Build command line.
    dst_dir = self.get_scratch_space()
    exec_file, notebook_file = _get_files()
    cmd = [
        f"{exec_file}",
        f"--dst_dir {dst_dir}",
        f"--notebook {notebook_file}",
    ]
    run_cmd_line(self, cmd, cmd_opts, dst_dir, exp, exp_pass)


# #############################################################################
# TestRunNotebook1
# #############################################################################


@pytest.mark.flaky(reruns=2)
@pytest.mark.skip(reason="Fix test run notebooks glitch CmTask #2792.")
class TestRunNotebook1(hunitest.TestCase):
    """
    Run notebooks without failures.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_notebook.0.log
        $SCRATCH_SPACE/result_0/simple_notebook.0.ipynb
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_notebook.1.log
        $SCRATCH_SPACE/result_1/simple_notebook.1.ipynb
        $SCRATCH_SPACE/result_1/success.txt"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - two notebooks (without any failure)
        - serially
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list1()'",
            "--num_threads 'serial'",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - with 2 threads
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list1()'",
            "--num_threads 2",
        ]
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################
# TestRunNotebook2
# #############################################################################


@pytest.mark.flaky(reruns=2)
@pytest.mark.skip(reason="Fix test run notebooks glitch CmTask #2792.")
class TestRunNotebook2(hunitest.TestCase):
    """
    Run experiments that fail.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_notebook.0.log
        $SCRATCH_SPACE/result_0/simple_notebook.0.ipynb
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_notebook.1.log
        $SCRATCH_SPACE/result_1/simple_notebook.1.ipynb
        $SCRATCH_SPACE/result_1/success.txt
        $SCRATCH_SPACE/result_2
        $SCRATCH_SPACE/result_2/config.pkl
        $SCRATCH_SPACE/result_2/config.txt
        $SCRATCH_SPACE/result_2/run_notebook.2.log"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - serially
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list2()'",
            "--num_threads 3",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_serial2(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - serially
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list2()'",
            "--skip_on_error",
            "--num_threads 3",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - with 2 threads
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list2()'",
            "--num_threads 2",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel2(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - with 2 threads
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_config_list2()'",
            "--skip_on_error",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################
# TestRunNotebook3
# #############################################################################


# TODO(Grisha): re-use `Test_Run_Notebook_TestCase`.
# TODO(Grisha): consider merging with `TestRunNotebook1` and / or `TestRunNotebook2`.
@pytest.mark.superslow("~35 sec.")
class TestRunNotebook3(hunitest.TestCase):
    def helper(
        self,
        fail: bool,
        allow_errors: bool,
        tee: bool,
    ) -> Tuple[int, str]:
        """
        Run the test notebook and get its log file.

        :param fail: the notebook breaks if `True` otherwise it does not break
        :param allow_errors: if `True`, run the notebook until the end
            regardless of any error in it.
            Note: if `True`, disables effects of `tee` in case it is enabled.
        :param tee: whether to fill log file with notebook errors or not
        :return: return code as int and log file path
        """
        # Get notebook path.
        amp_dir = hgit.get_amp_abs_path()
        input_dir = self.get_input_dir(use_only_test_class=True)
        notebook_name = "simple_notebook.ipynb"
        notebook_path = os.path.join(amp_dir, input_dir, notebook_name)
        # Build scratch dir and a path to the log file.
        dst_dir = self.get_scratch_space()
        log_file_path = os.path.join(dst_dir, "result_0", "run_notebook.0.log")
        # Get path to the tested script.
        script_path = os.path.join(
            amp_dir, "dev_scripts/notebooks", "run_notebook.py"
        )
        # Build a command to run the notebook.
        opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1"
        config_builder = (
            f"dev_scripts.test.test_run_notebook._config_builder({fail})"
        )
        cmd_run_txt = [
            f"{script_path}",
            f"--notebook {notebook_path}",
            f"--config_builder '{config_builder}'",
            f"--dst_dir {dst_dir}",
            f"{opts}",
        ]
        if tee:
            cmd_run_txt.insert(4, "--tee")
        if allow_errors:
            cmd_run_txt.insert(4, "--allow_errors")
        cmd_run_txt = " ".join(cmd_run_txt)
        cmd_txt = []
        cmd_txt.append(cmd_run_txt)
        cmd_txt = "\n".join(cmd_txt)
        _LOG.debug("cmd=%s", cmd_txt)
        # Exucute.
        rc = hsystem.system(
            cmd_txt,
            abort_on_error=False,
            suppress_output=False,
            log_level="echo",
        )
        _LOG.debug("rc=%s", rc)
        # Check if notebook is published.
        cmd = f"find {dst_dir} -name '*.html'"
        _, file_path = hsystem.system_to_string(cmd)
        _LOG.debug("file_path=%s", file_path)
        hdbg.dassert_file_exists(file_path)
        return rc, log_file_path

    def test1(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are allowed
        - The log file does not contain the errors, because they are allowed
        - Notebook is published
        """
        fail = True
        allow_errors = True
        tee = False
        # Check that script has passed without any errors.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 0
        self.assertEqual(actual_rc, expected_rc)

    def test2(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are not allowed
        - The log file does not contain the errors
        - Notebook is published
        """
        fail = True
        allow_errors = False
        tee = False
        # Check that script has passed with detected error.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 1
        self.assertEqual(actual_rc, expected_rc)

    def test3(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are not allowed
        - The log file contains the notebook errors
        - Notebook is published
        """
        fail = True
        allow_errors = False
        tee = True
        # Check that script has passed with detected error.
        actual_rc, log_file_path = self.helper(fail, allow_errors, tee)
        expected_rc = 1
        self.assertEqual(actual_rc, expected_rc)
        # Check that log file contains error message.
        log_content = hio.from_file(log_file_path)
        self.check_string(log_content, purify_text=True)

    def test4(self) -> None:
        """
        Test the notebook run with no broken cells.

        - Errors in the notebook are not allowed
        - The log file does not contain the notebook errors
        - Notebook is published
        """
        fail = False
        allow_errors = False
        tee = False
        # Check that script has passed without any errors.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 0
        self.assertEqual(actual_rc, expected_rc)

class Test_run_notebooks(hunitest.TestCase):
    mock_os_path_exists = umock.patch.object(os.path, "exists")
    mock_load_config_from_pickle = umock.patch.object(cconfig, "load_config_from_pickle")
    mock_hio_from_json = umock.patch.object(hio, "from_json")
    mock_run_notebook = umock.patch.object(run_experiment_notebooks, "_run_notebook")
    mock_publish_notebook = umock.patch.object(run_experiment_notebooks, "publish_system_reconciliation_notebook")
    ctx = umock.MagicMock(spec=Context)

    @pytest.fixture(autouse=True)
    def setup_teardown_test(self) -> Generator[Any, Any, Any]:
        self.set_up_test()
        yield
        self.tear_down_test()

    def set_up_test(self) -> None:
        self.os_mock: umock.MagicMock = self.mock_os_path_exists.start()
        self.config_mock: umock.MagicMock = self.mock_load_config_from_pickle.start()
        self.json_mock: umock.MagicMock = self.mock_hio_from_json.start()
        self.run_notebook_mock: umock.MagicMock = self.mock_run_notebook.start()
        self.run_notebook_mock.return_value = None
        self.publish_notebook_mock: umock.MagicMock = self.mock_publish_notebook.start()
        self.publish_notebook_mock.return_value = None

    def tear_down_test(self) -> None:
        self.mock_os_path_exists.stop()
        self.mock_load_config_from_pickle.stop()
        self.mock_hio_from_json.stop()
        self.mock_run_notebook.stop()

    def test1(self) -> None:
        """
        Test for a full system run scenario where the config file for a full system run exists.
        """
        self.os_mock.return_value = True
        self.json_mock.return_value = {}
        self.config_mock.return_value = {
            "dag_runner_config" : {
                "bar_duration_in_secs" : 1800,
            },
            "market_data_config" : {
                "universe_version" : "test_universe",
                "im_client_config" : {
                    "table_name" : "ABC",
                },
            },
            "process_forecasts_node_dict" : {
                "process_forecasts_dict" : {
                    "order_config" : {
                        "execution_frequency" : "hourly",
                    }
                }
            },
            "portfolio_config" : {
                "mark_to_market_config" : "40"
            }
        }
        run_experiment_notebooks.run_notebooks(
            ctx = self.ctx,
            system_log_dir = "/shared_data/ecs/test/system_reconciliation/C5b/prod/20240108_170500.20240108_173000/system_log_dir.manual/process_forecasts",
            base_dst_dir="/notebooks_output",
        )

        self.run_notebook_mock.assert_called_once()

    def test2(self) -> None:
        """
        Test for a broker-only run scenario where the config file for a broker-only run exists.
        """
        self.os_mock.return_value = False
        self.config_mock.return_value = {}
        self.json_mock.return_value = {
            "parent_order_duration_in_min": 32,
            "universe" : "test_universe",
            "child_order_execution_freq" : "hourly",
        }

        run_experiment_notebooks.run_notebooks(
            ctx=self.ctx,
            system_log_dir="/shared_data/ecs/test/20240110_experiment1",
            base_dst_dir="/notebooks_output"
        )

        self.run_notebook_mock.assert_called_once()

    def test3(self) -> None:
        """
        Test for a scenario where the config file doesn't exist.
        """
        self.os_mock.return_value = False
        self.config_mock.return_value = {}
        self.json_mock.return_value = {}
        run_experiment_notebooks.run_notebooks(
            ctx=self.ctx,
            system_log_dir="/non_existing_dir",
            base_dst_dir="/notebooks_output"
        )

        self.run_notebook_mock.assert_not_called()

    def test4(self) -> None:
        """
        Test for a scenario where the config file exists but is missing some required keys.
        """
        # Missing "dag_runner_config" key.
        self.config_mock.return_value = {
            "market_data_config" : {
                "universe_version" : "test_universe",
                "im_client_config" : {
                    "table_name" : "ABC",
                },
            },
            "process_forecasts_node_dict" : {
                "process_forecasts_dict" : {
                    "order_config" : {
                        "execution_frequency" : "hourly",
                    }
                }
            },
            "portfolio_config" : {
                "mark_to_market_config" : "40"
            }
        }
        self.os_mock.return_value = True
        self.json_mock.return_value = {}
        run_experiment_notebooks.run_notebooks(
            ctx = self.ctx,
            system_log_dir="/shared_data/ecs/test/system_reconciliation/C5b/prod/20240108_170500.20240108_173000/system_log_dir.manual/process_forecasts",
            base_dst_dir="/notebooks_output"
        )
        self.run_notebook_mock.assert_not_called()

    def test5(self) -> None:
        """
        Test for a scenario where the cconfig.load_config_from_pickle function returns an empty dictionary.
        """
        self.os_mock.return_value = True
        self.json_mock.return_value = {}
        self.config_mock.return_value = {}
        run_experiment_notebooks.run_notebooks(
            ctx=self.ctx,
            system_log_dir="/shared_data/ecs/test/system_reconciliation/C5b/prod/20240108_170500.20240108_173000/system_log_dir.manual/process_forecasts",
            base_dst_dir="/notebooks_output"
        )
        self.run_notebook_mock.assert_not_called()


    

    



    

