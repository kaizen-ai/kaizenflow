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
import core.finance as cofinanc
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hpickle as hpickle
import helpers.hsystem as hsystem

_LOG = logging.getLogger(__name__)


def compute_target_positions_in_cash(
    df: pd.DataFrame,
    *,
    bulk_frac_to_remove: float,
    bulk_fill_method: str,
    target_gmv: float = 100000,
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
    #
    predictions = df["prediction"].rename(0).to_frame().T
    volatility = df["volatility"].rename(0).to_frame().T
    target_positions_config = cconfig.get_config_from_flattened_dict(
        {
            "bulk_frac_to_remove": bulk_frac_to_remove,
            "bulk_fill_method": bulk_fill_method,
            "target_gmv": target_gmv,
            "volatility_lower_bound": volatility_lower_bound,
        }
    )
    target_positions = cofinanc.compute_target_positions_cross_sectionally(
        predictions, volatility, target_positions_config
    )
    hdbg.dassert_eq(target_positions.shape[0], 1)
    target_positions = pd.Series(
        target_positions.values[0],
        index=target_positions.columns,
        name="target_position",
        dtype="float",
    )
    if bulk_fill_method == "zero":
        target_positions = target_positions.fillna(0)
    else:
        raise ValueError("`bulk_fill_method`=%s not supported", bulk_fill_method)
    _LOG.debug(
        "`target_positions`=\n%s",
        hpandas.df_to_str(
            target_positions, print_dtypes=True, print_shape_info=True
        ),
    )
    # These positions are expressed in dollars.
    current_positions = df["position"]
    _LOG.debug(
        "`current_positions`=\n%s",
        hpandas.df_to_str(
            current_positions, print_dtypes=True, print_shape_info=True
        ),
    )
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
