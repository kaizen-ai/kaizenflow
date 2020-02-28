import core.config as cfg
import core.config_builders as ccfgbld
import helpers.unit_test as ut
import pandas as pd


class TestBuildMultipleConfigs(ut.TestCase):
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


class Test_config_comparisons(ut.TestCase):
    @staticmethod
    def get_test_config_1():
        config = cfg.Config()
        tmp_config = config.add_subconfig("build_model")
        tmp_config["activation"] = "sigmoid"
        tmp_config = config.add_subconfig("build_targets")
        tmp_config["target_asset"] = "Crude Oil"
        tmp_config = config["build_targets"].add_subconfig("preprocessing")
        tmp_config["preprocessor"] = "tokenizer"
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
        return config

    def test_check_same_configs_error(self):
        # Create list of configs with duplicates.
        configs = [
            Test_config_comparisons.get_test_config_1(),
            Test_config_comparisons.get_test_config_1(),
            Test_config_comparisons.get_test_config_2(),
        ]
        # Check
        with self.assertRaises(AssertionError):
            ccfgbld.check_same_configs(configs)

    def test_config_intersection(self):
        # Create expected intersection config.
        intersection_config = cfg.Config()
        tmp_config = intersection_config.add_subconfig("build_model")
        tmp_config["build_model"] = "sigmoid"
        tmp_config = intersection_config.add_subconfig("build_targets")
        tmp_config = intersection_config["build_targets"].add_subconfig(
            "preprocessing"
        )
        tmp_config["preprocessor"] = "tokenizer"
        config_1 = Test_config_comparisons.get_test_config_1()
        config_2 = Test_config_comparisons.get_test_config_2()
        actual_intersection = ccfgbld.get_config_intersection(
            [config_1, config_2]
        )
        self.assertEqual(str(intersection_config), str(actual_intersection))

    def test_varying_config_difference(self):
        # Create two different configs.
        config_1 = Test_config_comparisons.get_test_config_1()
        config_2 = Test_config_comparisons.get_test_config_2()
        # Define expected variation.
        expected_difference = {"build_targets.target_asset": ["Crude Oil", "Gold"]}
        actual_difference = ccfgbld.get_config_difference([config_1, config_2])
        self.assertEqual(expected_difference, actual_difference)

    def test_same_config_difference(self):
        config = Test_config_comparisons.get_test_config_1()
        actual_difference = ccfgbld.get_config_difference([config, config])
        # Check that the difference is empty.
        self.assertFalse(actual_difference)


class Test_get_config_dataframe(ut.TestCase):
    @staticmethod
    def get_test_config_1():
        config = cfg.Config()
        tmp_config = config.add_subconfig("build_model")
        tmp_config["activation"] = "sigmoid"
        tmp_config = config.add_subconfig("build_targets")
        tmp_config["target_asset"] = "Crude Oil"
        tmp_config = config["build_targets"].add_subconfig("preprocessing")
        tmp_config["preprocessor"] = "tokenizer"
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
        return config

    def test_all_params(self):
        config_1 = Test_get_config_dataframe.get_test_config_1()
        config_2 = Test_get_config_dataframe.get_test_config_2()

        expected_result = pd.DataFrame({'build_model.activation': ["sigmoid", "sigmoid"],
                                        'build_targets.target_asset': ['Crude Oil', 'Gold'],
                                        'build_targets.preprocessing.preprocessor': ['tokenizer', 'tokenizer']})
        actual_result = ccfgbld.get_configs_dataframe([config_1, config_2])
        self.assertTrue(expected_result.equals(actual_result))

    def test_different_params_subset(self):
        config_1 = Test_get_config_dataframe.get_test_config_1()
        config_2 = Test_get_config_dataframe.get_test_config_2()

        expected_result = pd.DataFrame({"build_targets.target_asset": ["Crude Oil", "Gold"]})
        actual_result = ccfgbld.get_configs_dataframe([config_1, config_2], params_subset="difference")
        self.assertTrue(expected_result.equals(actual_result))

    def test_custom_params_subset(self):
        config_1 = Test_get_config_dataframe.get_test_config_1()
        config_2 = Test_get_config_dataframe.get_test_config_2()

        expected_result = pd.DataFrame({"build_model.activation": ["sigmoid", "sigmoid"]})
        actual_result = ccfgbld.get_configs_dataframe([config_1, config_2], params_subset=["build_model.activation"])
        self.assertTrue(expected_result.equals(actual_result))

