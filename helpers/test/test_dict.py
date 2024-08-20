import logging
from typing import Any, Dict

import core.config as cconfig
import helpers.hdict as hdict
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_nested_dict_iterator(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test basic case with no nesting.
        """
        dict_ = {"key0": "value0", "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0",), "value0"), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test2(self) -> None:
        """
        Test simple nested case.
        """
        dict_ = {
            "key0": {"key00": "value00", "key01": "value01"},
            "key1": "value1",
        }
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [
            (("key0", "key00"), "value00"),
            (("key0", "key01"), "value01"),
            (("key1",), "value1"),
        ]
        self.assertListEqual(actual_result, expected_result)

    def test3(self) -> None:
        """
        Test multilevel nested case.
        """
        dict_ = {"key0": {"key00": {"key000": "value000"}}, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [
            (("key0", "key00", "key000"), "value000"),
            (("key1",), "value1"),
        ]
        self.assertListEqual(actual_result, expected_result)

    def test4(self) -> None:
        """
        Test flat case with `None` value.
        """
        dict_ = {"key0": "value0", "key1": None}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0",), "value0"), (("key1",), None)]
        self.assertListEqual(actual_result, expected_result)

    def test5(self) -> None:
        """
        Test nested case with `None` value.
        """
        dict_ = {"key0": {"key00": None}, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0", "key00"), None), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test6(self) -> None:
        """
        Test flat case with empty dict value.
        """
        dict_ = {"key0": {}, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0",), {}), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test7(self) -> None:
        """
        Test nested case with empty dict value.
        """
        dict_ = {"key0": {"key00": {}}, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0", "key00"), {}), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test8(self) -> None:
        """
        Test flat case with empty Config value.
        """
        config = cconfig.Config()
        dict_ = {"key0": config, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0",), config), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test9(self) -> None:
        """
        Test nexted case with empty Config value.
        """
        config = cconfig.Config()
        dict_ = {"key0": {"key00": config}, "key1": "value1"}
        actual_result = list(hdict.get_nested_dict_iterator(dict_))
        expected_result = [(("key0", "key00"), config), (("key1",), "value1")]
        self.assertListEqual(actual_result, expected_result)


class Test_typed_get(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that the function doesn't raise an exception when key is present
        in the dictionary and the retrieved value's type matched expected type.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key0", expected_type=str)
        # Check output.
        expected = "value0"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test that the function doesn't raise an exception when key is not
        present in the dictionary and a default value is provided.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key2", default_value="default_value")
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test that the function doesn't raise an exception when key is not
        present in the dictionary, default value if provided and retrieved
        value's type matches the expected type.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(
            dict_, "key2", default_value="default_value", expected_type=str
        )
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test4(self) -> None:
        """
        Test that function raises an exception when the key is present in the
        dictionary and retrieved value's type doesn't match the expected type.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.typed_get(dict_, "key0", expected_type=int)

    def test5(self) -> None:
        """
        Test that the function raises an exception when the key is not present
        in the dictionary and no default value is provided.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.typed_get(dict_, "key2")

    def test6(self) -> None:
        """
        Test that the function doesn't raise an exception when the key is
        present in the dictionary.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key1")
        # Check output.
        expected = "value1"
        self.assert_equal(actual, expected)

    def test7(self) -> None:
        """
        Test that the function doesn't raise an exception when dictionary is
        empty and default value is provided.
        """
        # Prepare inputs.
        dict_: Dict = {}
        # Run.
        actual = hdict.typed_get(dict_, "key1", default_value="default_value")
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test8(self) -> None:
        """
        Test that the function raises an exception when dictionary is empty and
        no default value.
        """
        # Prepare inputs.
        dict_: Dict = {}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.typed_get(dict_, "key1")

    def test9(self) -> None:
        """
        Test that the function doesn't raise an exception when dictionary is
        nested dictionary and key is present.
        """
        # Prepare inputs.
        dict_: Dict = {
            "key0": {"nestkey0": 1, "nestkey1": 2},
            "key1": "value1",
        }
        # Run.
        actual = hdict.typed_get(dict_, "key0", expected_type=dict)
        # Check output.
        expected = {"nestkey0": 1, "nestkey1": 2}
        self.assertDictEqual(actual, expected)

    def test10(self) -> None:
        """
        Test that the function doesn't raise an exception when key is present
        in the dictionary and the retrieved value is None.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": None, "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key0", expected_type=type(None))
        # Check output.
        expected = None
        self.assertEqual(actual, expected)

    def test11(self) -> None:
        """
        Test that the function raises an exception with custom object that
        behaves like dictionary.
        """

        # Prepare inputs.
        class Config:
            def __init__(self, **entries: Any) -> None:
                self.__dict__.update(entries)

            def __getitem__(self, key: Any) -> Any:
                return self.__dict__[key]

        config = Config(a=1, b=2)
        # Run.
        with self.assertRaises(AssertionError):
            hdict.typed_get(config, "a", expected_type=int)


class Test_checked_get(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that the function doesn't raise an exception when key is present
        in the dictionary.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.checked_get(dict_, "key0")
        # Check output.
        expected = "value0"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test that the function raises an exception when key is not present in
        the dictionary.
        """
        # Prepare inputs.
        dict_: Dict = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.checked_get(dict_, "key2")

    def test3(self) -> None:
        """
        Test that the function raises an exception when the dictionary is
        empty.
        """
        # Prepare inputs.
        dict_: Dict = {}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.checked_get(dict_, "key0")

    def test4(self) -> None:
        """
        Test that the function raises an exception with key of different type.
        """
        # Prepare inputs.
        dict_: Dict = {"0": "value0", "1": "value1"}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.checked_get(dict_, 1)

    def test5(self) -> None:
        """
        Test that the function raises an exception with key of None type.
        """
        # Prepare inputs.
        dict_: Dict = {"0": "value0", "1": "value1"}
        # Run.
        with self.assertRaises(AssertionError):
            hdict.checked_get(dict_, None)
