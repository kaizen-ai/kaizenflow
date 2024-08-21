import logging

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
        Test that the function doesn't raise an exception when the key is
        present in the dictionary.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key1")
        # Check output.
        expected = "value1"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test that the function raises an exception when dictionary is empty and
        no default value.
        """
        # Prepare inputs.
        dict_ = {}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key1")
        # Check output.
        actual_exp = str(e.exception)
        expected_exp = """
        * Failed assertion *
        'key1' in '{}'
        """
        self.assert_equal(actual_exp, expected_exp, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test that the function raises an exception when the provided dict_ is
        not a dictionary but list.
        """
        # Prepare inputs.
        dict_ = []
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key1")
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        Instance of '[]' is '<class 'list'>' instead of '<class 'dict'>'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test4(self) -> None:
        """
        Test that the function raises an exception when the provided dict_ is
        not a dictionary but set.
        """
        # Prepare inputs.
        dict_ = set()
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key1")
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        Instance of 'set()' is '<class 'set'>' instead of '<class 'dict'>'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test5(self) -> None:
        """
        Test that the function raises an exception when the provided dict_ is
        not a dictionary but integer.
        """
        # Prepare inputs.
        dict_ = 42
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key1")
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        Instance of '42' is '<class 'int'>' instead of '<class 'dict'>'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test6(self) -> None:
        """
        Test that the function raises an exception when the provided dict_ is
        not a dictionary but string.
        """
        # Prepare inputs.
        dict_ = "dict"
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key1")
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        Instance of 'dict' is '<class 'str'>' instead of '<class 'dict'>'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test7(self) -> None:
        """
        Test that the function doesn't raise an exception when key is not
        present in the dictionary and a default value is provided.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key2", default_value="default_value")
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test8(self) -> None:
        """
        Test that the function doesn't raise an exception when dictionary is
        empty and default value is provided.
        """
        # Prepare inputs.
        dict_ = {}
        # Run.
        actual = hdict.typed_get(dict_, "key1", default_value="default_value")
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test9(self) -> None:
        """
        Test that the function doesn't raise an exception when the key is
        present in the dictionary and the value's type matches the expected
        type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key0", expected_type=str)
        # Check output.
        expected = "value0"
        self.assert_equal(actual, expected)

    def test10(self) -> None:
        """
        Test that the function does not raise an exception when the key is
        missing, the default value is used, and the retrieved value's type
        matches the expected type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(
            dict_, "key2", default_value="default_value", expected_type=str
        )
        # Check output.
        expected = "default_value"
        self.assert_equal(actual, expected)


class Test_checked_get(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that the function doesn't raise an exception when key is present
        in the dictionary.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
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
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.checked_get(dict_, "key2")
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        'key2' in '{'key0': 'value0', 'key1': 'value1'}'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test that the function raises an exception with key of different type.
        """
        # Prepare inputs.
        dict_ = {"0": "value0", "1": "value1"}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.checked_get(dict_, 1)
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        '1' in '{'0': 'value0', '1': 'value1'}'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test4(self) -> None:
        """
        Test that the function raises an exception with key of None type.
        """
        # Prepare inputs.
        dict_ = {"0": "value0", "1": "value1"}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.checked_get(dict_, None)
        # Check output.
        actual = str(e.exception)
        expected = r"""
        * Failed assertion *
        'None' in '{'0': 'value0', '1': 'value1'}'
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
