"""
Import as:

import oms.call_optimizer as ocalopti
"""

import logging
import os

import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpickle as hpickle
import helpers.hsystem as hsystem
import helpers.lib_tasks as hlibtask

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

    :param tmp_dir: local dir to use to exchange parameters with the "remote"
        optimizer
    """
    hio.create_dir(tmp_dir, incremental=True)
    # Serialize the inputs in tmp_dir.
    input_obj = {"config": config, "df": df}
    input_file = os.path.join(tmp_dir, "input.pkl")
    hpickle.to_pickle(input_obj, input_file)
    # Login in the Docker on AWS to pull the `opt` image.
    # TODO(Grisha): we should call `invoke docker_login` CmTask #547.
    docker_login_cmd = "sudo -s eval $(aws ecr get-login --profile am --no-include-email --region us-east-1)"
    hsystem.system(docker_login_cmd)
    # Call optimizer_stub through Docker.
    cmd = []
    cmd.append("cd optimizer && ls")
    cmd.append("/app/optimizer/optimizer_stub.py")
    cmd.append(f"--input_file {input_file}")
    output_file = os.path.join(tmp_dir, "output.pkl")
    cmd.append(f"--output_file {output_file}")
    cmd.append("-v INFO")
    cmd = " ".join(cmd)
    #
    base_image = ""
    stage = "dev"
    version = ""
    entrypoint = True
    as_user = True
    # TODO(Grisha): ideally we should call `opt_docker_cmd -c cmd` CmTask #547.
    docker_cmd = hlibtask._get_docker_cmd(
        base_image, stage, version, cmd, entrypoint=entrypoint, as_user=as_user, use_bash=True
    )
    hsystem.system("sudo " + docker_cmd)
    # Read the output from `tmp_dir`.
    output_file = os.path.join(tmp_dir, "output.pkl")
    output_df = hpickle.from_pickle(output_file)
    return output_df


# TODO(gp): Move it to lib_tasks.
# ECR_BASE_PATH = os.environ["AM_ECR_BASE_PATH"]
ECR_BASE_PATH = "665840871993.dkr.ecr.us-east-1.amazonaws.com"


default_params = {
    "ECR_BASE_PATH": ECR_BASE_PATH,
    # When testing a change to the build system in a branch you can use a different
    # image, e.g., `XYZ_tmp` to not interfere with the prod system.
    # "BASE_IMAGE": "opt_tmp",
    "BASE_IMAGE": "opt",
    "DEV_TOOLS_IMAGE_PROD": f"{ECR_BASE_PATH}/dev_tools:prod",
    "USE_ONLY_ONE_DOCKER_COMPOSE": True,
}


hlibtask.set_default_params(default_params)
