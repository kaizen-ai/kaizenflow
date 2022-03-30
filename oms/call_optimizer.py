"""
Import as:

import oms.call_optimizer as ocalopti
"""

import logging
import os
from typing import List

import invoke
import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hpickle as hpickle
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def compute_target_positions_in_cash(
    df: pd.DataFrame,
    *,
    target_gmv: float = 100000,
    dollar_neutrality: str = "no_constraint",
    volatility_lower_bound: float = 1e-5,
) -> pd.DataFrame:
    """
    Compute target trades from holdings (dollar-valued) and predictions.

    This is a stand-in for optimization. This function does not have access to
    prices and so does not perform any conversions to or from shares. It also
    needs to be told the id associated with cash.

    :param df: a dataframe with current positions (in dollars) and predictions
    :return: a dataframe with target positions and trades
        (denominated in dollars)
    """
    # Sanity-check the dataframe.
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert(not df.empty)
    hdbg.dassert_is_subset(
        ["asset_id", "prediction", "volatility", "position"], df.columns
    )
    hdbg.dassert_not_in("target_position", df.columns)
    hdbg.dassert_not_in("target_trade", df.columns)
    #
    hdbg.dassert(not df["prediction"].isna().any())
    hdbg.dassert(not df["volatility"].isna().any())
    hdbg.dassert(not df["position"].isna().any())
    #
    df = df.set_index("asset_id")
    hdbg.dassert(not df.index.has_duplicates)
    # In this placeholder, we maintain two invariants (approximately):
    #   1. Net wealth is conserved from one step to the next.
    #   2. GMV is conserved from one step to the next.
    # The second invariant may be restated as conserving gross exposure.
    predictions = df["prediction"]
    _LOG.debug("predictions=\n%s", predictions)
    volatility = df["volatility"]
    _LOG.debug("volatility=\n%s", volatility)
    # Set a lower bound on the volatility forecast.
    volatility = volatility.clip(lower=volatility_lower_bound)
    # Calculate volatility-weighted target positions. This is not yet scaled
    #  to target GMV.
    unscaled_target_positions = predictions.divide(volatility)
    if dollar_neutrality == "no_constraint":
        pass
    elif dollar_neutrality == "demean":
        hdbg.dassert_lt(
            1,
            unscaled_target_positions.count(),
            "More than one asset required to enforce dollar neutrality.",
        )
        net_target_position = unscaled_target_positions.mean()
        _LOG.debug(
            "Target net asset value prior to dollar neutrality constaint=%f"
            % net_target_position
        )
        unscaled_target_positions -= net_target_position
    elif dollar_neutrality == "side_preserving":
        hdbg.dassert_lt(
            1,
            unscaled_target_positions.count(),
            "More than one asset required to enforce dollar neutrality.",
        )
        positive_asset_value = unscaled_target_positions.clip(lower=0).sum()
        hdbg.dassert_lt(0, positive_asset_value, "No long predictions provided.")
        negative_asset_value = -1 * unscaled_target_positions.clip(upper=0).sum()
        hdbg.dassert_lt(0, negative_asset_value, "No short predictions provided.")
        min_sided_asset_value = min(positive_asset_value, negative_asset_value)
        positive_scale_factor = min_sided_asset_value / positive_asset_value
        negative_scale_factor = min_sided_asset_value / negative_asset_value
        positive_positions = (
            positive_scale_factor * unscaled_target_positions.clip(lower=0)
        )
        negative_positions = (
            negative_scale_factor * unscaled_target_positions.clip(upper=0)
        )
        unscaled_target_positions = positive_positions.add(negative_positions)
    else:
        raise ValueError(
            "Unrecognized option `dollar_neutrality`=%s" % dollar_neutrality
        )
    unscaled_target_positions_l1 = unscaled_target_positions.abs().sum()
    _LOG.debug("unscaled_target_positions_l1 =%s", unscaled_target_positions_l1)
    hdbg.dassert_lte(0, unscaled_target_positions_l1)
    if unscaled_target_positions_l1 == 0:
        _LOG.debug("All target positions are zero.")
        scale_factor = 0
    else:
        scale_factor = target_gmv / unscaled_target_positions_l1
    _LOG.debug("scale_factor=%s", scale_factor)
    # These positions are expressed in dollars.
    current_positions = df["position"]
    net_wealth = current_positions.sum()
    _LOG.debug("net_wealth=%s", net_wealth)
    # Drop cash.
    target_positions = scale_factor * unscaled_target_positions
    target_trades = target_positions - current_positions
    df["target_position"] = target_positions
    df["target_notional_trade"] = target_trades
    return df


def run_optimizer(
    config: cconfig.Config,
    df: pd.DataFrame,
    *,
    tmp_dir: str = "tmp.optimizer_stub",
) -> pd.DataFrame:
    """
    Run the optimizer through Docker.

    The flow is:
       - Save the input data in a temp dir
       - Start an `opt` Docker container
       - Run the optimizer
       - Save the optimizer output to a temp dir

    :param tmp_dir: local dir to use to exchange parameters with the "remote"
        optimizer
    """
    # Login in the Docker on AWS to pull the `opt` image.
    # TODO(Grisha): Move this inside the `opt_docker_cmd`.
    # TODO(Grisha): maybe move `docker_login` to the entrypoint?
    # To avoid to call init_logger overwriting the call to it from `main`.
    import helpers.lib_tasks as hlibtask

    ctx = invoke.context.Context()
    hlibtask.docker_login(ctx)
    # Serialize the inputs in `tmp_dir`.
    hio.create_dir(tmp_dir, incremental=True)
    input_obj = {"config": config, "df": df}
    input_file = os.path.join(tmp_dir, "input.pkl")
    hpickle.to_pickle(input_obj, input_file)
    # Get path to the `optimizer_stub.py`.
    root_dir = hgit.get_client_root(False)
    optimizer_stub_file_path = os.path.join(
        root_dir, "optimizer/optimizer_stub.py"
    )
    hdbg.dassert_file_exists(optimizer_stub_file_path)
    # Build the command to be executed in `opt` container.
    docker_cmd_: List[str] = []
    docker_cmd_.append(optimizer_stub_file_path)
    docker_cmd_.append(f"--input_file {input_file}")
    output_file = os.path.join(tmp_dir, "output.pkl")
    docker_cmd_.append(f"--output_file {output_file}")
    docker_cmd_.append("-v INFO")
    docker_cmd = " ".join(docker_cmd_)
    # Call `optimizer_stub` through `opt` Docker container.
    optimizer_cmd_: List[str] = []
    # `opt` invokes can only be run from `optimizer` dir.
    optimizer_cmd_.append("cd optimizer &&")
    optimizer_cmd_.append(f"invoke opt_docker_cmd --cmd '{docker_cmd}'")
    optimizer_cmd = " ".join(optimizer_cmd_)
    # TODO(Grisha): call `opt_docker_cmd` directly.
    hsystem.system(optimizer_cmd)
    # Read the output from `tmp_dir`.
    output_file = os.path.join(tmp_dir, "output.pkl")
    output_df = hpickle.from_pickle(output_file)
    return output_df
