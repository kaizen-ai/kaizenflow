from typing import Any, Dict, Iterable, List, Tuple, Generator
import unittest.mock as umock
from invoke import Context
import pytest
import tempfile
import os
import json
import pickle
import helpers.hunit_test as hunitest
import dev_scripts.lib_tasks_run_model_experiment_notebooks as run_experiment_notebooks

class Test_run_notebooks(hunitest.TestCase):
    mock__run_notebook = umock.patch.object(run_experiment_notebooks, "_run_notebook")
    mock_publish_notebook = umock.patch.object(run_experiment_notebooks, "publish_system_reconciliation_notebook")
    ctx = umock.MagicMock(spec=Context)

    @pytest.fixture(autouse=True)
    def setup_teardown_test(self) -> Generator[Any, Any, Any]:
        self.ctx = umock.MagicMock(spec=Context)
        self.set_up_test()
        yield
        self.tear_down_test()

    def set_up_test(self) -> None:
        self.run__notebook_mock: umock.MagicMock = self.mock__run_notebook.start()
        self.run__notebook_mock.return_value = None
        self.publish_notebook_mock: umock.MagicMock = self.mock_publish_notebook.start()
        self.publish_notebook_mock.return_value = None

    def tear_down_test(self) -> None:
        self.mock__run_notebook.stop()
        self.mock_publish_notebook.stop()

    def test1(self) -> None:
        """
        Broker only run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            broker_only_config = {
                "parent_order_duration_in_min": 32,
                "universe" : "test_universe",
                "child_order_execution_freq": "hourly",
            }

            with open(os.path.join(temp_dir, "args.json"), "w") as f:
                json.dump(broker_only_config, f)

            run_experiment_notebooks.run_notebooks(
                ctx = self.ctx,
                system_log_dir=temp_dir,
                base_dst_dir="/notebooks_output"
            )
            self.run__notebook_mock.assert_called_once()

    def test2(self) -> None:
        """
        full system config run.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            full_system_config = {
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

            with open(os.path.join(temp_dir, "config.pkl"), "wb") as f:
                pickle.dump(full_system_config, f)

            run_experiment_notebooks.run_notebooks(
                ctx = self.ctx,
                system_log_dir=temp_dir,
                base_dst_dir="/notebooks_output"
            )
            self.run__notebook_mock.assert_called_once()

    