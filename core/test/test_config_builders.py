import pprint
from typing import List, Optional, cast

import core.config as cconfig
import helpers.unit_test as hut


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


class TestGetConfigsFromBuilder1(hut.TestCase):
    def test1(self) -> None:
        """
        Build a config from.
        """
        config_builder = "core.test.test_config_builders._build_test_configs()"
        configs = cconfig.get_configs_from_builder(config_builder)
        txt = pprint.pformat(configs)
        self.check_string(txt)


# #############################################################################


class TestGetConfigFromEnv(hut.TestCase):
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


# #############################################################################


class Test_generate_default_config_variants1(hut.TestCase):
    def test_add_var_params(self) -> None:
        """
        Verify that Cartesian product of configs with varying parameters is
        what expected.
        """
        # Prepare varying parameters.
        params_variants = {("build_targets", "target_asset"): ["Gasoil", "Soy"]}
        # Pass test config builder to generating function.
        actual_configs = cconfig.generate_default_config_variants(
            _get_test_config_1, params_variants
        )
        # Convert configs to string for comparison.
        actual_configs = [str(config) for config in actual_configs]
        # Manually add varying params to test configs.
        expected_config_1 = _get_test_config_1()
        expected_config_1[("build_targets", "target_asset")] = "Gasoil"
        expected_config_2 = _get_test_config_1()
        expected_config_2[("build_targets", "target_asset")] = "Soy"
        # Convert configs to string for comparison.
        expected_configs = [str(expected_config_1), str(expected_config_2)]
        # Compare config lists element-wise.
        self.assertEqual(expected_configs, actual_configs)


# #############################################################################


# TODO(gp): -> Test_build_multiple_configs1
class TestBuildMultipleConfigs(hut.TestCase):
    def test_existing_path(self) -> None:
        # Create config template.
        config_template = cconfig.Config()
        config_tmp = config_template.add_subconfig("read_data")
        config_tmp["symbol"] = None
        config_tmp = config_template.add_subconfig("resample")
        config_tmp["rule"] = None
        # Define parameters.
        params_variants = {
            ("read_data", "symbol"): ["CL", "QM"],
            ("resample", "rule"): ["5T", "7T", "10T"],
        }
        # Check the results.
        actual_result = cconfig.build_multiple_configs(
            config_template, params_variants
        )
        self.check_string(str(actual_result))

    def test_non_existent_path(self) -> None:
        # Create config template.
        config_template = cconfig.Config()
        config_tmp = config_template.add_subconfig("read_data")
        config_tmp["symbol"] = None
        config_tmp = config_template.add_subconfig("resample")
        config_tmp["rule"] = None
        # Define parameters.
        params_variants = {
            ("read_data", "symbol_bug"): ["CL", "QM"],
            ("resample", "rule"): ["5T", "7T", "10T"],
        }
        # Check the results.
        with self.assertRaises(ValueError):
            _ = cconfig.build_multiple_configs(config_template, params_variants)

    def test_not_nan_parameter(self) -> None:
        # Create config template.
        config_template = cconfig.Config()
        config_tmp = config_template.add_subconfig("read_data")
        config_tmp["symbol"] = "CL"
        config_tmp = config_template.add_subconfig("resample")
        config_tmp["rule"] = None
        # Define parameters.
        params_variants = {
            ("read_data", "symbol"): ["CL", "QM"],
            ("resample", "rule"): ["5T", "7T", "10T"],
        }
        # Check the results.
        with self.assertRaises(ValueError):
            _ = cconfig.build_multiple_configs(config_template, params_variants)
