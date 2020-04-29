import collections
import pprint
from typing import List, Optional, cast

import pandas as pd

import core.config as cfg
import core.config_builders as ccfgbld
import helpers.unit_test as hut

# #############################################################################
# core/config_builders.py
# #############################################################################


class TestGetConfigFromFlattened1(hut.TestCase):
    def test1(self) -> None:
        flattened = collections.OrderedDict(
            [
                (("read_data", "file_name"), "foo_bar.txt"),
                (("read_data", "nrows"), 999),
                (("single_val",), "hello"),
                (("zscore", "style"), "gaz"),
                (("zscore", "com"), 28),
            ]
        )
        config = ccfgbld.get_config_from_flattened(flattened)
        self.check_string(str(config))

    def test2(self) -> None:
        flattened = collections.OrderedDict(
            [
                (("read_data", "file_name"), "foo_bar.txt"),
                (("read_data", "nrows"), 999),
                (("single_val",), "hello"),
                (("zscore",), cfg.Config()),
            ]
        )
        config = ccfgbld.get_config_from_flattened(flattened)
        self.check_string(str(config))


class TestGetConfigFromNestedDict1(hut.TestCase):
    def test1(self) -> None:
        nested = {
            "read_data": {"file_name": "foo_bar.txt", "nrows": 999,},
            "single_val": "hello",
            "zscore": {"style": "gaz", "com": 28,},
        }
        config = ccfgbld.get_config_from_nested_dict(nested)
        self.check_string(str(config))

    def test2(self) -> None:
        nested = {
            "read_data": {"file_name": "foo_bar.txt", "nrows": 999,},
            "single_val": "hello",
            "zscore": cfg.Config(),
        }
        config = ccfgbld.get_config_from_nested_dict(nested)
        self.check_string(str(config))


def _build_test_configs(symbols: Optional[List[str]] = None,) -> List[cfg.Config]:
    config_template = cfg.Config()
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


class TestGetConfigsFromBuilder1(hut.TestCase):
    def test1(self) -> None:
        """
        Build a config from
        """
        config_builder = "core.test.test_config_builders._build_test_configs()"
        configs = ccfgbld.get_configs_from_builder(config_builder)
        txt = pprint.pformat(configs)
        self.check_string(txt)


class TestGetConfigFromEnv(hut.TestCase):
    def test_no_env_variables(self) -> None:
        """
        Verify that if there are no config env variables, no config is created.
        """
        # Test that no config is created.
        actual_config = ccfgbld.get_config_from_env()
        self.assertTrue(actual_config is None)


# #############################################################################


class TestBuildMultipleConfigs(hut.TestCase):
    def test_existing_path(self) -> None:
        # Create config template.
        config_template = cfg.Config()
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
        actual_result = ccfgbld.build_multiple_configs(
            config_template, params_variants
        )
        self.check_string(str(actual_result))

    def test_non_existent_path(self) -> None:
        # Create config template.
        config_template = cfg.Config()
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
            _ = ccfgbld.build_multiple_configs(config_template, params_variants)

    def test_not_nan_parameter(self) -> None:
        # Create config template.
        config_template = cfg.Config()
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
            _ = ccfgbld.build_multiple_configs(config_template, params_variants)


def _get_test_config_1() -> cfg.Config:
    """
    Build a test config for Crude Oil asset.
    :return: Test config.
    """
    config = cfg.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Crude Oil"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config


def _get_test_config_2() -> cfg.Config:
    """
    Build a test config for Gold asset.
    :return: Test config.
    """
    config = cfg.Config()
    tmp_config = config.add_subconfig("build_model")
    tmp_config["activation"] = "sigmoid"
    tmp_config = config.add_subconfig("build_targets")
    tmp_config["target_asset"] = "Gold"
    tmp_config = config["build_targets"].add_subconfig("preprocessing")
    tmp_config["preprocessor"] = "tokenizer"
    tmp_config = config.add_subconfig("meta")
    tmp_config["experiment_result_dir"] = "results.pkl"
    return config


class TestCheckSameConfigs(hut.TestCase):
    def test_check_same_configs_error(self) -> None:
        """
        Verify that an error is raised when same configs are encountered.
        """
        # Create list of configs with duplicates.
        configs = [
            _get_test_config_1(),
            _get_test_config_1(),
            _get_test_config_2(),
        ]
        # Make sure fnction raises an error.
        with self.assertRaises(AssertionError):
            ccfgbld.assert_on_duplicated_configs(configs)


class TestConfigIntersection(hut.TestCase):
    def test_different_config_intersection(self) -> None:
        """
        Verify that intersection of two different configs is what expected.
        """
        # Prepare actual output of intersection function.
        # TODO(*): Bad unit testing fomr! What are these configs?
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        intersection = ccfgbld.get_config_intersection([config_1, config_2])
        self.check_string(str(intersection))

    def test_same_config_intersection(self) -> None:
        """
        Verify that intersection of two same configs equals those configs.
        """
        # Prepare test config.
        # TODO(*): Bad unit testing form! What is this config?
        test_config = _get_test_config_1()
        # FInd intersection of two same configs.
        actual_intersection = ccfgbld.get_config_intersection(
            [test_config, test_config]
        )
        # Verify that intersection is equal to initial config.
        self.assertEqual(str(test_config), str(actual_intersection))


class TestConfigDifference(hut.TestCase):
    def test_varying_config_difference(self) -> None:
        """
        Verify that differing parameters of different configs are what expected.
        """
        # Create two different configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Compute variation between configs.
        actual_difference = ccfgbld.get_config_difference([config_1, config_2])
        # Define expected variation.
        expected_difference = {
            "build_targets.target_asset": ["Crude Oil", "Gold"]
        }
        self.assertEqual(expected_difference, actual_difference)

    def test_same_config_difference(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        # Create test config.
        config = _get_test_config_1()
        # Compute difference between two instances of same config.
        actual_difference = ccfgbld.get_config_difference([config, config])
        # Verify that the difference is empty.
        self.assertFalse(actual_difference)


class TestGetConfigDataframe(hut.TestCase):
    """
    Compare manually constructed dfs and dfs created by `ccfgbld.get_configs_dataframe`
    using `pd.DataFrame.equals()`
    """

    def test_all_params(self) -> None:
        """
        Compute and verify dataframe with all config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to dataframe.
        actual_result = ccfgbld.get_configs_dataframe([config_1, config_2])
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {
                "build_model.activation": ["sigmoid", "sigmoid"],
                "build_targets.target_asset": ["Crude Oil", "Gold"],
                "build_targets.preprocessing.preprocessor": [
                    "tokenizer",
                    "tokenizer",
                ],
                "meta.experiment_result_dir": ["results.pkl", "results.pkl"],
            }
        )
        self.assertTrue(expected_result.equals(actual_result))

    def test_different_params_subset(self) -> None:
        """
        Compute and verify dataframe with all only varying config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to df, keeping only varying params.
        actual_result = ccfgbld.get_configs_dataframe(
            [config_1, config_2], params_subset="difference"
        )
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_targets.target_asset": ["Crude Oil", "Gold"]}
        )
        self.assertTrue(expected_result.equals(actual_result))

    def test_custom_params_subset(self) -> None:
        """
        Compute and verify dataframe with arbitrary config parameters.
        """
        # Get two test configs.
        config_1 = _get_test_config_1()
        config_2 = _get_test_config_2()
        # Convert configs to df, keeping arbitrary parameter.
        actual_result = ccfgbld.get_configs_dataframe(
            [config_1, config_2], params_subset=["build_model.activation"]
        )
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_model.activation": ["sigmoid", "sigmoid"]}
        )
        self.assertTrue(expected_result.equals(actual_result))


class TestAddResultDir(hut.TestCase):
    def test_result_dir(self) -> None:
        """
        `Verify that `ccfgbld.add_result_dir` adds correct value to correct param path.
        """
        result_dir = "test/results"
        # Modify test config manually.
        expected_config = _get_test_config_1()
        expected_config[("meta", "result_dir")] = result_dir
        # Pass test config as one-item list and apply function.
        actual_config = [_get_test_config_1()]
        actual_config = ccfgbld.add_result_dir(result_dir, actual_config)
        # Unpack and check.
        actual_config = actual_config[0]
        self.assertEqual(str(expected_config), str(actual_config))


class TestSetExperimentResultDir(hut.TestCase):
    def test_set_experiment_result_dir(self) -> None:
        """
        Verify that we add correct value with `set_experiment_result_dir`.
        """
        # Prepare result dir name.
        sim_dir = "/data/tests/test_results"
        actual_config = _get_test_config_1()
        # Set using function.
        actual_config = ccfgbld.set_experiment_result_dir(sim_dir, actual_config)
        # Set result file name manually.
        expected_config = _get_test_config_1()
        expected_config[
            ("meta", "experiment_result_dir")
        ] = "/data/tests/test_results"
        self.assert_equal(str(expected_config), str(actual_config))


class TestAddConfigIdx(hut.TestCase):
    def test_add_config_idx(self) -> None:
        """
        Verify that `ccfgbld.add_config_idx` adds correct index to correct param path.
        """
        # Assign id parameters through function.
        actual_configs = [
            _get_test_config_1(),
            _get_test_config_1(),
        ]
        actual_configs = ccfgbld.add_config_idx(actual_configs)
        # Convert configs to string for comparison.
        actual_configs = [str(config) for config in actual_configs]
        # Assign id parameters manually.
        expected_config_1 = _get_test_config_1()
        expected_config_1[("meta", "id")] = 0
        expected_config_2 = _get_test_config_1()
        expected_config_2[("meta", "id")] = 1
        # Convert configs to string for comparison.
        expected_configs = [str(expected_config_1), str(expected_config_2)]
        # Compare config lists element-wise.
        self.assertEqual(expected_configs, actual_configs)


class TestGenerateDefaultConfigVariants(hut.TestCase):
    def test_add_var_params(self) -> None:
        """
        Verify that Cartesian product of configs with varying parameters
        is what expected.
        """
        # Prepare varying parameters.
        params_variants = {("build_targets", "target_asset"): ["Gasoil", "Soy"]}
        # Pass test config builder to generating function.
        actual_configs = ccfgbld.generate_default_config_variants(
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
