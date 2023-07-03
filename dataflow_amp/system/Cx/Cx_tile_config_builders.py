"""
Import as:

import dataflow_amp.system.Cx.Cx_tile_config_builders as dtfascctcbu
"""

import dataflow.system as dtfsys

# TODO(Grisha): This is not specific of Cx, consider using `build_tile_config_list()`
# for all train-test modes and killing the function.
def build_tile_config_list(
    system: dtfsys.System, train_test_mode: str
) -> dtfsys.SystemConfigList:
    """
    Build a `SystemConfigList` object based on `train_test_mode`.

    :param train_test_mode: same as in `Cx_NonTime_ForecastSystem`
    """
    if train_test_mode in ["ins", "rolling"]:
        # Partition by time and asset_ids.
        system_config_list = dtfsys.build_tile_config_list(system)
    elif train_test_mode == "ins_oos":
        # TODO(Grisha): consider partitioning by asset_ids in case of
        # memory issues.
        # TODO(Grisha): P1, document why not using `dtfsys.build_tile_config_list(system)`.
        system_config_list = dtfsys.SystemConfigList.from_system(system)
    else:
        raise ValueError(f"Invalid train_test_mode='{train_test_mode}'")
    return system_config_list
