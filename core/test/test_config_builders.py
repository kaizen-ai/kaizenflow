import core.config as cfg
import core.config_builders as cfgb
import helpers.unit_test as ut


class TestBuildMultipleConfigs(ut.TestCase):
    def test_existing_path(self):
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
        actual_result = cfgb.build_multiple_configs(
            config_template, params_variants
        )
        self.check_string(str(actual_result))

    def test_non_existent_path(self):
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
            actual_result = cfgb.build_multiple_configs(
                config_template, params_variants
            )

    def test_not_nan_parameter(self):
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
            actual_result = cfgb.build_multiple_configs(
                config_template, params_variants
            )
