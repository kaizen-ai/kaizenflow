import logging
import pprint

import core.config as cfg
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_config1(hut.TestCase):
    def test_config1(self) -> None:
        """
        Test print flatten config.
        """
        config = cfg.Config()
        config["hello"] = "world"
        self.check_string(str(config))

    def test_config2(self) -> None:
        """
        Test serialization / deserialization for flat config.
        """
        config = self._get_flat_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    def test_config3(self) -> None:
        """
        Test Config.get()
        """
        config = cfg.Config()
        config["nrows"] = 10000
        #
        self.assertEqual(config["nrows"], 10000)
        self.assertEqual(config.get("nrows", None), 10000)
        #
        self.assertEqual(config.get("nrows_tmp", None), None)

    def test_get2(self) -> None:
        """
        Test Config.get() with missing key.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]
        with self.assertRaises(AssertionError):
            _ = config["read_data"]["file_name2"]
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]["file_name2"]
        elem = config["read_data"]["file_name"]
        self.assertEqual(elem, "foo_bar.txt")

    def test_config4(self) -> None:
        """
        Test print nested config.
        """
        config = self._get_nested_config1()
        act = str(config)
        self.check_string(act)

    def test_config5(self) -> None:
        """
        Test to_python() nested config.
        """
        config = self._get_nested_config1()
        act = config.to_python()
        self.check_string(act)

    def test_config6(self) -> None:
        """
        Test serialization / deserialization for nested config.
        """
        config = self._get_nested_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    def test_config7(self) -> None:
        """
        Compare two different styles of building a nested config.
        """
        config1 = self._get_nested_config1()
        config2 = self._get_nested_config2()
        #
        self.assertEqual(str(config1), str(config2))

    def test_hierarchical_getitem1(self) -> None:
        """
        Test accessing the config with hierarchical access.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        elem1 = config[("read_data", "file_name")]
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_getitem2(self) -> None:
        """
        Test accessing the config with hierarchical access with correct and
        incorrect paths.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2", "file_name")]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2")]
        with self.assertRaises(AssertionError):
            _ = config[["read_data2"]]
        #
        elem = config[("read_data", "file_name")]
        self.assertEqual(elem, "foo_bar.txt")

    def test_hierarchical_get1(self) -> None:
        """
        Show that hierarchical access is equivalent to chained access.
        """
        config = self._get_nested_config1()
        elem1 = config.get(("read_data", "file_name"), None)
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_get2(self) -> None:
        """
        Test `get()` with hierarchical access.
        """
        config = self._get_nested_config1()
        elem = config.get(("read_data2", "file_name"), "hello_world1")
        self.assertEqual(elem, "hello_world1")
        elem = config.get(("read_data2", "file_name2"), "hello_world2")
        self.assertEqual(elem, "hello_world2")
        elem = config.get(("read_data", "file_name2"), "hello_world3")
        self.assertEqual(elem, "hello_world3")

    def test_hierarchical_update1(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("write_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val2"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore2")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config1.update(config2)
        self.check_string(str(config1))

    def test_hierarchical_update2(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("read_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore")
        config_tmp["style"] = "super"
        #
        config_tmp = config2.add_subconfig("extra_zscore")
        config_tmp["style"] = "universal"
        config_tmp["tau"] = 32
        #
        config1.update(config2)
        self.check_string(str(config1))

    def test_hierarchical_update_empty_nested_config1(self) -> None:
        """
        Generate a config of `{"key1": {"key0": }}` structure.
        """
        subconfig = cfg.Config()
        subconfig.add_subconfig("key0")
        #
        config = cfg.Config()
        config_tmp = config.add_subconfig("key1")
        config_tmp.update(subconfig)
        #
        expected_result = cfg.Config()
        config_tmp = expected_result.add_subconfig("key1")
        config_tmp.add_subconfig("key0")
        self.assertEqual(str(config), str(expected_result))

    def test_config_with_function(self) -> None:
        config = cfg.Config()
        config[
            "filters"
        ] = "[(<function _filter_relevance at 0x7fe4e35b1a70>, {'thr': 90})]"
        expected_result = "filters: [(<function _filter_relevance>, {'thr': 90})]"
        actual_result = str(config)
        self.assertEqual(actual_result, expected_result)

    def test_config_with_object(self) -> None:
        config = cfg.Config()
        config[
            "dag_builder"
        ] = "<dataflow_p1.task2538_pipeline.ArPredictorBuilder object at 0x7fd7a9ddd190>"
        expected_result = "dag_builder: <dataflow_p1.task2538_pipeline.ArPredictorBuilder object>"
        actual_result = str(config)
        self.assertEqual(actual_result, expected_result)

    def test_flatten1(self) -> None:
        config = cfg.Config()
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config_tmp = config.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        flattened = config.flatten()
        # TODO(*): `sort_dicts` param new in 3.8.
        # string = pprint.pformat(flattened, sort_dicts=False)
        string = pprint.pformat(flattened)
        self.check_string(string)

    def test_flatten2(self) -> None:
        config = cfg.Config()
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config.add_subconfig("zscore")
        #
        flattened = config.flatten()
        # TODO(*): `sort_dicts` param new in 3.8.
        # string = pprint.pformat(flattened, sort_dicts=False)
        string = pprint.pformat(flattened)
        self.check_string(string)

    def _check_python(self, config: cfg.Config) -> str:
        code = config.to_python()
        _LOG.debug("code=%s", code)
        config2 = cfg.Config.from_python(code)
        #
        act = []
        act.append("config=%s" % str(config))
        act.append("code=%s" % str(code))
        act.append("config2=%s" % str(config2))
        act = "\n".join(act)
        self.assertEqual(str(config), str(config2))
        return act

    @staticmethod
    def _get_flat_config1() -> cfg.Config:
        config = cfg.Config()
        config["hello"] = "world"
        config["foo"] = [1, 2, 3]
        return config

    @staticmethod
    def _get_nested_config1() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config.add_subconfig("read_data")
        config["read_data"]["file_name"] = "foo_bar.txt"
        config["read_data"]["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config.add_subconfig("zscore")
        config["zscore"]["style"] = "gaz"
        config["zscore"]["com"] = 28
        return config

    @staticmethod
    def _get_nested_config2() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config_tmp = config.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        return config


class Test_subtract_config1(hut.TestCase):
    def test_test1(self) -> None:
        config1 = cfg.Config()
        config1[("l0",)] = "1st_floor"
        config1[["l1", "l2"]] = "2nd_floor"
        config1[["r1", "r2", "r3"]] = [1, 2]
        #
        config2 = cfg.Config()
        config2["l0"] = "Euro_1nd_floor"
        config2[["l1", "l2"]] = "2nd_floor"
        config2[["r1", "r2", "r3"]] = [1, 2]
        #
        diff = cfg.subtract_config(config1, config2)
        self.check_string(str(diff))

    def test_test2(self) -> None:
        config1 = cfg.Config()
        config1[("l0",)] = "1st_floor"
        config1[["l1", "l2"]] = "2nd_floor"
        config1[["r1", "r2", "r3"]] = [1, 2, 3]
        #
        config2 = cfg.Config()
        config2["l0"] = "Euro_1nd_floor"
        config2[["l1", "l2"]] = "2nd_floor"
        config2[["r1", "r2", "r3"]] = [1, 2]
        config2["empty"] = cfg.Config()
        #
        diff = cfg.subtract_config(config1, config2)
        self.check_string(str(diff))
