import pprint
from typing import Any, Dict

import helpers.hunit_test as hunitest
import im_v2.airflow.dags.airflow_utils.trade_exec.utils as imvadauteu


class Test_build_system_run_cmd(hunitest.TestCase):
    @staticmethod
    def get_params() -> Dict[str, Any]:
        """
        Get params and their values to build system run cmd.

        :return: a dictionary containing parameters and their values
        """
        params = {
            "strategy": "C11a",
            "dag_builder_ctor_as_str": "dataflow_lemonade.pipelines.C11.C11a_pipeline.C11a_DagBuilder",
            "trade_date": "20240717",
            "liveness": "CANDIDATE",
            "instance_type": "PROD",
            "exchange": "binance",
            "stage": "preprod",
            "account_type": "trading",
            "secret_id": 3,
            "verbosity": "DEBUG",
            "run_duration": "86400",
            "run_mode": "paper_trading",
            "start_time": "20240717_123000",
            "log_base_dir": "/efs/preprod/system_reconciliation/C11a.config1/paper_trading/20240717_123000.20240718_122500",
            "dag_run_mode": "scheduled",
            "set_config_value_params": None,
        }
        return params

    def test1(self) -> None:
        """
        Check that system run cmd is built correctly when all the parameters
        are passed.
        """
        # Set inputs.
        params = self.get_params()
        params["set_config_value_params"] = [
            '--set_config_value \'("dag_property_config","debug_mode_config","save_node_io"),(str(""))\'',
            '--set_config_value \'("dag_property_config","debug_mode_config","save_node_df_out_stats"),(bool(False))\'',
        ]
        # Run.
        actual_cmd = imvadauteu.build_system_run_cmd(**params)
        actual = pprint.pformat(actual_cmd)
        expected = r"""
        ['/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py',
        "--strategy 'C11a'",
        '--dag_builder_ctor_as_str '
        "'dataflow_lemonade.pipelines.C11.C11a_pipeline.C11a_DagBuilder'",
        '--trade_date 20240717',
        "--liveness 'CANDIDATE'",
        "--instance_type 'PROD'",
        "--exchange 'binance'",
        "--stage 'preprod'",
        "--account_type 'trading'",
        "--secret_id '3'",
        "-v 'DEBUG' 2>&1",
        '--run_duration 86400',
        "--run_mode 'paper_trading'",
        "--start_time '20240717_123000'",
        '--log_file_name '
        "'/efs/preprod/system_reconciliation/C11a.config1/paper_trading/20240717_123000.20240718_122500/logs/log.scheduled.txt'",
        '--dst_dir '
        "'/efs/preprod/system_reconciliation/C11a.config1/paper_trading/20240717_123000.20240718_122500/system_log_dir.scheduled'",
        '--set_config_value '
        '\'("dag_property_config","debug_mode_config","save_node_io"),(str(""))\'',
        '--set_config_value '
        '\'("dag_property_config","debug_mode_config","save_node_df_out_stats"),(bool(False))\'']
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that system run cmd is built correctly when only required
        parameters are passed.
        """
        # Set inputs.
        params = self.get_params()
        # Run.
        actual_cmd = imvadauteu.build_system_run_cmd(**params)
        actual = pprint.pformat(actual_cmd)
        expected = r"""
        ['/app/amp/dataflow_amp/system/Cx/scripts/run_Cx_prod_system.py',
        "--strategy 'C11a'",
        '--dag_builder_ctor_as_str '
        "'dataflow_lemonade.pipelines.C11.C11a_pipeline.C11a_DagBuilder'",
        '--trade_date 20240717',
        "--liveness 'CANDIDATE'",
        "--instance_type 'PROD'",
        "--exchange 'binance'",
        "--stage 'preprod'",
        "--account_type 'trading'",
        "--secret_id '3'",
        "-v 'DEBUG' 2>&1",
        '--run_duration 86400',
        "--run_mode 'paper_trading'",
        "--start_time '20240717_123000'",
        '--log_file_name '
        "'/efs/preprod/system_reconciliation/C11a.config1/paper_trading/20240717_123000.20240718_122500/logs/log.scheduled.txt'",
        '--dst_dir '
        "'/efs/preprod/system_reconciliation/C11a.config1/paper_trading/20240717_123000.20240718_122500/system_log_dir.scheduled'"]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_log_full_bid_ask_data_cmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that a cmd to log bid ask data is built correctly.
        """
        # Set input.
        universe = "v8.1"
        data_vendor = "CCXT"
        exchange_id = "binance"
        # Run.
        actual_cmd = imvadauteu.get_log_full_bid_ask_data_cmd(
            universe, data_vendor, exchange_id
        )
        actual = pprint.pformat(actual_cmd)
        expected = r"""
        ['amp/im_v2/ccxt/db/log_experiment_data.py',
        "--db_stage 'preprod'",
        "--start_timestamp_as_str '{}'",
        "--end_timestamp_as_str '{}'",
        "--log_dir '{}/system_log_dir.{}/process_forecasts'",
        "--data_vendor 'CCXT'",
        "--universe 'v8.1'",
        "--exchange_id 'binance'"]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_flatten_account_cmd(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that flatten account cmd is built correctly when all the
        parameters are passed.
        """
        # Set inputs.
        exchange = "binance"
        secret_id = 3
        universe = "v8.1"
        log_dir = "/efs/preprod/system_reconciliation/logs"
        # Run.
        actual_cmd = imvadauteu.get_flatten_account_cmd(
            exchange,
            secret_id,
            universe,
            log_dir,
        )
        actual = pprint.pformat(actual_cmd)
        expected = r"""
        ['amp/oms/broker/ccxt/scripts/flatten_ccxt_account.py',
        '--log_dir /efs/preprod/system_reconciliation/logs',
        "--exchange 'binance'",
        "--contract_type 'futures'",
        "--stage 'prod'",
        '--secret_id 3',
        '--assert_on_non_zero',
        "--universe 'v8.1'"]
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
