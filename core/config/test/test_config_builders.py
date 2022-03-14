import pprint
from typing import List, Optional, cast

import core.config as cconfig
import helpers.hunit_test as hunitest


def _build_test_configs(
    symbols: Optional[List[str]] = None,
) -> List[cconfig.Config]:
    config_template = cconfig.Config()
    config_tmp = config_template.add_subconfig("read_data")
    config_tmp["symbol"] = None
    config_tmp = config_template.add_subconfig("resample")
    config_tmp["rule"] = None
    #
    configs = []
    if not symbols:
        symbols = ["H He O C Bk"]
    symbols = cast(List[str], symbols)
    for symbol in symbols:
        config = config_template.copy()
        config[("read_data", "symbol")] = symbol
        configs.append(config)
    return configs


# #############################################################################


class TestGetConfigsFromBuilder1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Build a config from.
        """
        config_builder = (
            "core.config.test.test_config_builders._build_test_configs()"
        )
        configs = cconfig.get_configs_from_builder(config_builder)
        txt = pprint.pformat(configs)
        self.check_string(txt)


# #############################################################################


class TestGetConfigFromEnv(hunitest.TestCase):
    def test_no_env_variables(self) -> None:
        """
        Verify that if there are no config env variables, no config is created.
        """
        # Test that no config is created.
        actual_config = cconfig.get_config_from_env()
        self.assertIs(actual_config, None)


# #############################################################################


# TODO(gp): This is repeated code. Consider unifying it.
def _get_test_config_1() -> cconfig.Config:
    """
    Build a test config for Crude Oil asset.

    :return: Test config.
    """
    config = cconfig.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Crude Oil"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config


def _get_test_config_2() -> cconfig.Config:
    """
    Build a test config for Gold asset.

    :return: Test config.
    """
    config = cconfig.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Gold"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config