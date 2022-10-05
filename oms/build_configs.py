"""
Import as:

import oms.build_configs as obuiconf
"""

# TODO(Grisha): @Dan Decide where to put the file.
import os

import core.config as cconfig
import helpers.hsystem as hsystem


def get_reconciliation_config(date_str: str, asset_class: str) -> cconfig.Config:
    """
    Get a reconciliation that is specific of an asset class.

    :param date_str: reconciliation date as str, e.g., `20221003`
    :param asset_class: either `equities` or `crypto`
    """
    # Set values for variables that are specific of an asset class.
    if asset_class == "crypto":
        # For crypto the TCA part is not implemented yet.
        run_tca = False
        #
        bar_duration = "5T"
        #
        root_dir = "/shared_data/prod_reconciliation"
        # TODO(Grisha): probably we should rename to `system_log_dir`.
        prod_dir = os.path.join(
            root_dir,
            date_str,
            "prod",
            "system_log_dir_scheduled__2022-10-03T10:00:00+00:00_2hours",
        )
        data_dict = {
            "prod_dir": prod_dir,
            # For crypto we do not have a `candidate` so we just re-use prod.
            "cand_dir": prod_dir,
            "sim_dir": os.path.join(
                root_dir, date_str, "simulation", "system_log_dir"
            ),
        }
        #
        fep_init_dict = {
            "price_col": "vwap",
            "prediction_col": "vwap.ret_0.vol_adj_2_hat",
            "volatility_col": "vwap.ret_0.vol",
        }
        quantization = "no_quantization"
        gmv = 700.0
        liquidate_at_end_of_day = False
    elif asset_class == "equities":
        run_tca = True
        #
        bar_duration = "15T"
        #
        root_dir = ""
        search_str = ""
        prod_dir_cmd = f"find {root_dir}/{date_str}/prod -name '{search_str}'"
        _, prod_dir = hsystem.system_to_string(prod_dir_cmd)
        cand_cmd = (
            f"find {root_dir}/{date_str}/job.candidate.* -name '{search_str}'"
        )
        _, cand_dir = hsystem.system_to_string(cand_cmd)
        data_dict = {
            "prod_dir": prod_dir,
            "cand_dir": cand_dir,
            "sim_dir": os.path.join(root_dir, date_str, "system_log_dir"),
        }
        #
        fep_init_dict = {
            "price_col": "twap",
            "prediction_col": "prediction",
            "volatility_col": "garman_klass_vol",
        }
        quantization = "nearest_share"
        gmv = 20000.0
        liquidate_at_end_of_day = True
    else:
        raise ValueError(f"Unsupported asset class={asset_class}")
    # Get a config.
    config_dict = {
        "meta": {
            "date_str": date_str,
            "asset_class": asset_class,
            "run_tca": run_tca,
            "bar_duration": bar_duration,
        },
        "load_data_config": data_dict,
        "research_forecast_evaluator_from_prices": {
            "init": fep_init_dict,
            "annotate_forecasts_kwargs": {
                "quantization": quantization,
                "burn_in_bars": 3,
                "style": "cross_sectional",
                "bulk_frac_to_remove": 0.0,
                "target_gmv": gmv,
                "liquidate_at_end_of_day": liquidate_at_end_of_day,
            },
        },
    }
    config = cconfig.Config.from_dict(config_dict)
    return config
