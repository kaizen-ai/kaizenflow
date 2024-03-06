"""
Import as:

import dataflow_amp.system.Cx.Cx_tile_config_builders as dtfascctcbu
"""

from typing import Optional

import dataflow.backtest as dtfbcktst
import dataflow.system as dtfsys
import dataflow_amp.system.Cx.Cx_forecast_system_example as dtfasccfsex


# TODO(Grisha): Deprecate after `amp/dataflow_amp/system/Cx/run_Cx_historical_simulation.sh`
# is deprecated.
# TODO(Grisha): ideally we should separate system construction from the config building, but since
# we use the `config_builder` as string when running scripts it makes it hard to pass a System
# object to `build_tile_config_list()`.
def get_Cx_config_builder_for_historical_simulations(
    dag_builder_ctor_as_str: str,
    fit_at_beginning: bool,
    train_test_mode: str,
    backtest_config: str,
    root_dir: str,
    *,
    oos_start_date_as_str: Optional[str] = None,
) -> dtfsys.SystemConfigList:
    """
    Get a config builder function to run historical simulations.

    An executable from `amp/dataflow_amp/system/Cx/run_Cx_historical_simulation.sh`
        - i.e. `run_config_list.py`
    requires a config building function.

    :param dag_builder_ctor_as_str: same as in `Cx_NonTime_ForecastSystem`
    :param fit_at_beginning: force the system to fit before making predictions
    :param train_test_mode: same as in `Cx_NonTime_ForecastSystem`
    :param backtest_config: see `apply_backtest_config()`
    :param oos_start_date_as_str: used only for train_test_mode="ins_oos",
        see `dtfasccfsex.apply_ins_oos_backtest_config()`
    """
    # TODO(Grisha): we should have a single `NonTimeForecastSystem` instance that we can use
    # for any model types, e.g., Cx, Ex, Mock.
    system = dtfasccfsex.get_Cx_NonTime_ForecastSystem_example(
        dag_builder_ctor_as_str,
        fit_at_beginning,
        train_test_mode=train_test_mode,
        backtest_config=backtest_config,
        root_dir=root_dir,
        oos_start_date_as_str=oos_start_date_as_str,
    )
    config_builder = dtfbcktst.build_tile_config_list(system, train_test_mode)
    return config_builder
