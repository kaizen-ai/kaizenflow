import pandas as pd

import core.config as cfg
import core.dataflow.core as cdc
import helpers.unit_test as hut

from typing import Tuple

def test_get_set_state(
    fit_df: pd.DataFrame, predict_df: pd.DataFrame, config: cfg.Config, node: cdc.Node
) -> Tuple[str, str]:
    """
    Helper for testing `get_fit_state()` and `set_fit_state()` methods.

    :param fit_df: dataframe used for fitting a node
    :param predict_df: dataframe used as node input for `predict()`
    :param config: config for initializing `node`
    :param node: dataflow node class
    :return: expected (generated from trained node), actual (generated from
        node initialized from state)
    """
    node1 = node("sklearn", **config.to_dict())
    # Fit model and get state.
    node1.fit(fit_df)
    state = node1.get_fit_state()
    # Predict using fitted node.
    df_out1 = node1.predict(predict_df)["df_out"]
    expected = hut.convert_df_to_string(df_out1.round(3), index=True)
    # Create a new node, set state, and predict.
    node2 = node("sklearn", **config.to_dict())
    node2.set_fit_state(state)
    df_out2 = node2.predict(predict_df)["df_out"]
    actual = hut.convert_df_to_string(df_out2.round(3), index=True)
    return expected, actual
