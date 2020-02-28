import pandas as pd
import pytest

import core.config as cfg
import core.config_builders as ccfgbld
import helpers.system_interaction as si
import helpers.unit_test as hut
import nlp.build_configs as ncfgbld


class ConfigTestHelper:
    @staticmethod
    def get_test_config_1():
        config = cfg.Config()
        tmp_config = config.add_subconfig("build_model")
        tmp_config["activation"] = "sigmoid"
        tmp_config = config.add_subconfig("build_targets")
        tmp_config["target_asset"] = "Crude Oil"
        tmp_config = config["build_targets"].add_subconfig("preprocessing")
        tmp_config["preprocessor"] = "tokenizer"
        tmp_config = config.add_subconfig("meta")
        tmp_config["result_file_name"] = "results.pkl"
        return config

    @staticmethod
    def get_test_config_2():
        config = cfg.Config()
        tmp_config = config.add_subconfig("build_model")
        tmp_config["activation"] = "sigmoid"
        tmp_config = config.add_subconfig("build_targets")
        tmp_config["target_asset"] = "Gold"
        tmp_config = config["build_targets"].add_subconfig("preprocessing")
        tmp_config["preprocessor"] = "tokenizer"
        tmp_config = config.add_subconfig("meta")
        tmp_config["result_file_name"] = "results.pkl"
        return config


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
            actual_result = ccfgbld.build_multiple_configs(
                config_template, params_variants
            )

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
            actual_result = ccfgbld.build_multiple_configs(
                config_template, params_variants
            )


class TestCheckSameConfigs(hut.TestCase):
    def test_check_same_configs_error(self):
        # Create list of configs with duplicates.
        configs = [
            ConfigTestHelper.get_test_config_1(),
            ConfigTestHelper.get_test_config_1(),
            ConfigTestHelper.get_test_config_2(),
        ]
        # Check if function raises an error.
        with self.assertRaises(AssertionError):
            ccfgbld.check_same_configs(configs)


class TestConfigIntersection(hut.TestCase):
    def test_config_intersection(self):
        # Create expected intersection config.
        intersection_config = cfg.Config()
        tmp_config = intersection_config.add_subconfig("build_model")
        tmp_config["activation"] = "sigmoid"
        tmp_config = intersection_config.add_subconfig("build_targets")
        tmp_config = intersection_config["build_targets"].add_subconfig(
            "preprocessing"
        )
        tmp_config["preprocessor"] = "tokenizer"
        tmp_config = intersection_config.add_subconfig("meta")
        tmp_config["result_file_name"] = "results.pkl"
        config_1 = ConfigTestHelper.get_test_config_1()
        config_2 = ConfigTestHelper.get_test_config_2()
        actual_intersection = ccfgbld.get_config_intersection(
            [config_1, config_2]
        )
        self.assertEqual(str(intersection_config), str(actual_intersection))


class TestConfigDifference(hut.TestCase):
    def test_varying_config_difference(self):
        # Create two different configs.
        config_1 = ConfigTestHelper.get_test_config_1()
        config_2 = ConfigTestHelper.get_test_config_2()
        # Define expected variation.
        expected_difference = {
            "build_targets.target_asset": ["Crude Oil", "Gold"]
        }
        actual_difference = ccfgbld.get_config_difference([config_1, config_2])
        self.assertEqual(expected_difference, actual_difference)

    def test_same_config_difference(self):
        config = ConfigTestHelper.get_test_config_1()
        actual_difference = ccfgbld.get_config_difference([config, config])
        # Check that the difference is empty.
        self.assertFalse(actual_difference)


class TestGetConfigDataframe(hut.TestCase):
    """
    Compare manually constructed dfs and dfs created by `ccfgbld.get_configs_dataframe`
    using `pd.DataFrame.equals()`
    """

    def test_all_params(self):
        # Get two test configs.
        config_1 = ConfigTestHelper.get_test_config_1()
        config_2 = ConfigTestHelper.get_test_config_2()

        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {
                "build_model.activation": ["sigmoid", "sigmoid"],
                "build_targets.target_asset": ["Crude Oil", "Gold"],
                "build_targets.preprocessing.preprocessor": [
                    "tokenizer",
                    "tokenizer",
                ],
                "meta.result_file_name": ["results.pkl", "results.pkl"],
            }
        )
        actual_result = ccfgbld.get_configs_dataframe([config_1, config_2])
        self.assertTrue(expected_result.equals(actual_result))

    def test_different_params_subset(self):
        # Get two test configs.
        config_1 = ConfigTestHelper.get_test_config_1()
        config_2 = ConfigTestHelper.get_test_config_2()
        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_targets.target_asset": ["Crude Oil", "Gold"]}
        )
        actual_result = ccfgbld.get_configs_dataframe(
            [config_1, config_2], params_subset="difference"
        )
        self.assertTrue(expected_result.equals(actual_result))

    def test_custom_params_subset(self):
        # Get two test configs.
        config_1 = ConfigTestHelper.get_test_config_1()
        config_2 = ConfigTestHelper.get_test_config_2()

        # Create expected dataframe and one with function.
        expected_result = pd.DataFrame(
            {"build_model.activation": ["sigmoid", "sigmoid"]}
        )
        actual_result = ccfgbld.get_configs_dataframe(
            [config_1, config_2], params_subset=["build_model.activation"]
        )
        self.assertTrue(expected_result.equals(actual_result))


class TestAddResultDir(hut.TestCase):
    def test_result_dir(self):
        result_dir = "test/results"
        # Modify test config manually.
        expected_config = ConfigTestHelper.get_test_config_1()
        expected_config[("meta", "result_dir")] = result_dir
        # Pass test config as one-item list and apply function.
        actual_config = [ConfigTestHelper.get_test_config_1()]
        actual_config = ccfgbld.add_result_dir(result_dir, actual_config)
        # Unpack and check.
        actual_config = actual_config[0]
        self.assertEqual(str(expected_config), str(actual_config))


class TestSetAbsoluteResultFilePath(hut.TestCase):
    def test_set_absolute_result_file_path(self):
        sim_dir = "/data/tests/test_results/"
        expected_config = ConfigTestHelper.get_test_config_1()
        # Set result file name manually.
        expected_config[
            ("meta", "result_file_name")
        ] = "/data/tests/test_results/results.pkl"
        # Set using function.
        actual_config = ConfigTestHelper.get_test_config_1()
        actual_config = ccfgbld.set_absolute_result_file_path(
            sim_dir, actual_config
        )
        self.assertEqual(str(expected_config), str(actual_config))


class TestAddConfigIdx(hut.TestCase):
    def test_add_config_idx(self):
        # Assign id parameters manually.
        expected_config_1 = ConfigTestHelper.get_test_config_1()
        expected_config_1[("meta", "id")] = 0
        expected_config_2 = ConfigTestHelper.get_test_config_1()
        expected_config_2[("meta", "id")] = 1
        # Assign id parameters through function.
        actual_configs = [
            ConfigTestHelper.get_test_config_1(),
            ConfigTestHelper.get_test_config_1(),
        ]
        actual_configs = ccfgbld.add_config_idx(actual_configs)
        # Convert configs to string for comparison.
        expected_configs = [str(expected_config_1), str(expected_config_2)]
        actual_configs = [str(config) for config in actual_configs]
        self.assertEqual(expected_configs, actual_configs)


class TestGenerateDefaultConfigVariants(hut.TestCase):
    def test_add_var_params(self):
        params_variants = {("build_targets", "target_asset"): ["Gasoil", "Soy"]}
        # Manually add varying params to test configs.
        expected_config_1 = ConfigTestHelper.get_test_config_1()
        expected_config_1[("build_targets", "target_asset")] = "Gasoil"
        expected_config_2 = ConfigTestHelper.get_test_config_1()
        expected_config_2[("build_targets", "target_asset")] = "Soy"
        # Pass test config builder to generating function.
        actual_configs = ccfgbld.generate_default_config_variants(
            ConfigTestHelper.get_test_config_1, params_variants
        )
        expected_configs = [str(expected_config_1), str(expected_config_2)]
        actual_configs = [str(config) for config in actual_configs]
        self.assertEqual(expected_configs, actual_configs)


class TestGetConfigFromEnv(hut.TestCase):
    def test_no_env_variables(self):
        # Test that no config is created.
        actual_config = ccfgbld.get_config_from_env()
        self.assertTrue(actual_config is None)

    @pytest.mark.skip
    # TODO(*) The function fails outside of notebook use. Update the test to reflect that.
    def test_with_env_variables(self):
        config_dir = "/data/test/"
        config_builder = "ncfgbld.build_PartTask1088_configs()"
        config_idx = "0"
        expected_config = ncfgbld.build_PartTask1088_configs()[0]
        expected_config[("meta", "result_dir")] = config_dir
        expected_config[("meta", "result_file_name")] = "/data/test/results.pkl"
        cmd = (
            'export __CONFIG_BUILDER__="%s"; export __CONFIG_IDX__=%s; export __DST_DIR__=%s'
            % (config_builder, config_idx, config_dir)
        )
        si.system(cmd)
        actual_config = ccfgbld.get_config_from_env()
        self.assertEqual(str(expected_config), str(actual_config))
