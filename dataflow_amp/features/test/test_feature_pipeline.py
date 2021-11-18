import logging

import core.config as cconfig
import core.dataflow as dtf
import dataflow_amp.features.pipeline as dtfamfepip
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestFeaturePipeline(hunitest.TestCase):
    """
    Test the feature pipeline end-to-end.
    """

    def test1(self) -> None:
        dag_builder = dtfamfepip.FeaturePipeline()
        #
        config = dag_builder.get_config_template()
        _LOG.debug("config from dag_builder=%s", config)
        # Initialize config.
        config["load_data"] = cconfig.get_config_from_nested_dict(
            {
                "source_node_name": "arma",
                "source_node_kwargs": {
                    "frequency": "T",
                    "start_date": "2010-01-04 09:30:00",
                    "end_date": "2010-01-04 16:05:00",
                    "ar_coeffs": [0],
                    "ma_coeffs": [0],
                    "scale": 0.1,
                    "burnin": 0,
                    "seed": 0,
                },
            }
        )
        config["perform_col_arithmetic", "func_kwargs"] = {"col_groups": []}
        config["zscore", "transformer_kwargs", "dyadic_tau"] = 5
        config["compress_tails", "transformer_kwargs", "scale"] = 6
        config["cross_feature_pairs", "func_kwargs"] = {
            "feature_groups": [
                (
                    "bid_size",
                    "ask_size",
                    [
                        "difference",
                        "normalized_difference",
                        "compressed_difference",
                    ],
                    "basize",
                )
            ],
            "join_output_with_input": False,
        }
        #
        _LOG.debug("config after patching=%s", config)
        # Initialize DAG runner.
        dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        df_str = hunitest.convert_df_to_string(
            df_out.round(3).dropna(), index=True, decimals=3
        )
        self.check_string(df_str)
