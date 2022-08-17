import collections

import pandas as pd

import core.config as cconfig
import helpers.hprint as hprint
import helpers.hunit_test as hunitest


def _get_test_config1() -> cconfig.Config:
    """
    Build a test config for Crude Oil asset.
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


def _get_test_config2() -> cconfig.Config:
    """
    Build a test config.

    Same as `_get_test_config1()` but with "Gold" instead of "Crude Oil"
    for target asset.
    """
    config = _get_test_config1().copy()
    config[("build_targets", "target_asset")] = "Gold"
    return config


def _get_test_config3() -> cconfig.Config:
    """
    Build a test config.
    """
    config = _get_test_config1().copy()
    config["hello"] = "world"
    return config


# #############################################################################
# Test_validate_configs1
# #############################################################################


# TODO(gp): -> validate_config_list
class Test_validate_configs1(hunitest.TestCase):
    def test_check_same_configs_error(self) -> None:
        """
        Verify that an error is raised when duplicated configs are encountered.
        """
        # Create list of configs with duplicates.
        configs = [
            _get_test_config1(),
            _get_test_config1(),
            _get_test_config2(),
        ]
        config_list = cconfig.ConfigList()
        # Make sure function raises an error.
        with self.assertRaises(AssertionError) as cm:
            config_list.configs = configs
        act = str(cm.exception)
        self.check_string(act, fuzzy_match=True)

    def test1(self) -> None:
        """
        Test configs without duplicates.
        """
        configs = [
            _get_test_config1(),
            _get_test_config2(),
            _get_test_config3(),
        ]
        config_list = cconfig.ConfigList()
        config_list.configs = configs
        config_list.validate_config_list()


# #############################################################################
# Test_intersect_configs1
# #############################################################################


class Test_intersect_configs1(hunitest.TestCase):
    def test_same_config(self) -> None:
        """
        Verify that intersection of two same configs equals those configs.
        """
        # Prepare test config.
        config = _get_test_config1()
        # FInd intersection of two same configs.
        actual = cconfig.intersect_configs([config, config])
        # Verify that intersection is equal to initial config.
        self.assertEqual(str(actual), str(config))

    def test1(self) -> None:
        """
        Verify that intersection of two different configs is what is expected.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        intersection = cconfig.intersect_configs([config1, config2])
        act = str(intersection)
        exp = r"""
        build_model:
          activation: sigmoid
        build_targets:
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl"""
        exp = hprint.dedent(exp)
        self.assert_equal(act, exp, fuzzy_match=False)


# #############################################################################
# Test_subtract_configs1
# #############################################################################


class Test_subtract_configs1(hunitest.TestCase):
    def test_same_config(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        config = _get_test_config1()
        diff = cconfig.subtract_config(config, config)
        # The difference should be empty.
        self.assertFalse(diff)

    def test1(self) -> None:
        """
        Verify that differing parameters of different configs are what
        expected.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        act = cconfig.subtract_config(config1, config2)
        exp = """
        build_targets:
          target_asset: Crude Oil"""
        exp = hprint.dedent(exp)
        self.assert_equal(str(act), str(exp))


# #############################################################################
# Test_diff_configs1
# #############################################################################


class Test_diff_configs1(hunitest.TestCase):
    def test_same_config(self) -> None:
        """
        Verify that the difference of two configs is empty.
        """
        config = _get_test_config1()
        act = cconfig.diff_configs([config, config])
        exp = [cconfig.Config(), cconfig.Config()]
        self.assert_equal(str(act), str(exp))

    def test1(self) -> None:
        config1 = _get_test_config1()
        exp = """
        build_model:
          activation: sigmoid
        build_targets:
          target_asset: Crude Oil
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl"""
        exp = hprint.dedent(exp)
        self.assert_equal(str(config1), exp)
        #
        config2 = _get_test_config2()
        exp = """
        build_model:
          activation: sigmoid
        build_targets:
          target_asset: Gold
          preprocessing:
            preprocessor: tokenizer
        meta:
          experiment_result_dir: results.pkl"""
        exp = hprint.dedent(exp)
        self.assert_equal(str(config2), exp)
        #
        act = cconfig.diff_configs([config1, config2])
        exp = [
            #
            cconfig.Config.from_dict(
                {"build_targets": {"target_asset": "Crude Oil"}}
            ),
            #
            cconfig.Config.from_dict(
                {"build_targets": {"target_asset": "Gold"}}
            ),
        ]
        self.assert_equal(str(act), str(exp))

    def test2(self) -> None:
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        config3 = _get_test_config3()
        #
        act = cconfig.diff_configs([config1, config2, config3])
        act = "\n".join(map(str, act))
        #
        exp = [
            #
            cconfig.Config.from_dict(
                {"build_targets": {"target_asset": "Crude Oil"}}
            ),
            #
            cconfig.Config.from_dict(
                {"build_targets": {"target_asset": "Gold"}}
            ),
            #
            cconfig.Config.from_dict(
                {"build_targets": {"target_asset": "Crude Oil"}, "hello": "world"}
            ),
        ]
        exp = "\n".join(map(str, exp))
        self.assert_equal(str(act), str(exp))


# #############################################################################
# Test_convert_to_dataframe1
# #############################################################################


class Test_convert_to_dataframe1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Compute and verify dataframe with all config parameters.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        # Convert configs to dataframe.
        act = cconfig.convert_to_dataframe([config1, config2])
        act = hunitest.convert_df_to_string(act, index=True)
        #
        exp = pd.DataFrame(
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
        exp = hunitest.convert_df_to_string(exp, index=True)
        self.assert_equal(str(act), str(exp))


# #############################################################################
# Test_build_config_diff_dataframe1
# #############################################################################


class Test_build_config_diff_dataframe1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Summarize differences between two different configs.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        #
        act = cconfig.build_config_diff_dataframe({"1": config1, "2": config2})
        act = hunitest.convert_df_to_string(act, index=True)
        #
        exp = pd.DataFrame(
            {
                "build_targets.target_asset": ["Crude Oil", "Gold"],
            }
        )
        exp = hunitest.convert_df_to_string(exp, index=True)
        self.assert_equal(str(act), str(exp))

    def test2(self) -> None:
        """
        Same config.
        """
        config1 = _get_test_config1()
        #
        act = cconfig.build_config_diff_dataframe({"1": config1, "2": config1})
        act = hunitest.convert_df_to_string(act, index=True)
        #
        exp = """
        Empty DataFrame
        Columns: []
        Index: [0, 1]
        """
        self.assert_equal(str(act), exp, fuzzy_match=True)

    def test3(self) -> None:
        """
        Three different configs.
        """
        config1 = _get_test_config1()
        config2 = _get_test_config2()
        config3 = _get_test_config3()
        #
        act = cconfig.build_config_diff_dataframe(
            {"1": config1, "2": config2, "3": config3}
        )
        act = hunitest.convert_df_to_string(act, index=True)
        #
        exp = """
          build_targets.target_asset  hello
        0                  Crude Oil    NaN
        1                       Gold    NaN
        2                  Crude Oil  world
        """
        self.assert_equal(str(act), exp, fuzzy_match=True)
