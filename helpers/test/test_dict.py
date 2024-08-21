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
        self.assertIsInstance(actual, str)
        expected = "value1"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test that the function raises an exception when key is not present in
        dictionary without default value.
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
        Test that the function raises an exception when a non-dict type is
        provided instead of dictionary.
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
        Test that the function doesn't raise an exception when key is not
        present in the dictionary with default value provided.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key2", default_value="default_value")
        # Check output.
        self.assertIsInstance(actual, str)
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test5(self) -> None:
        """
        Test that the function doesn't raise an exception when key is not
        present in the dictionary with default value as None.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key2", default_value=None)
        # Check output.
        expected = None
        self.assertIsNone(actual, expected)

    def test6(self) -> None:
        """
        Test that the function doesn't raise an exception when the key is
        present in the dictionary and value is of the expected type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(dict_, "key0", expected_type=str)
        # Check output.
        self.assertIsInstance(actual, str)
        expected = "value0"
        self.assert_equal(actual, expected)

    def test7(self) -> None:
        """
        Test that the function raises an exception when the key is
        present in the dictionary and the value is not of the expected type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(dict_, "key0", expected_type=int)
        # Check output.
        actual_exp = str(e.exception)
        expected_exp = r"""
        * Failed assertion *
        Instance of 'value0' is '<class 'str'>' instead of '<class 'int'>'
        """
        self.assert_equal(actual_exp, expected_exp, fuzzy_match=True)

    def test8(self) -> None:
        """
        Test that the function doesn't raise an exception when the key is not
        present in dictionary and default value matches expected type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        actual = hdict.typed_get(
            dict_, "key2", default_value="default_value", expected_type=str
        )
        # Check output.
        self.assertIsInstance(actual, str)
        expected = "default_value"
        self.assert_equal(actual, expected)

    def test9(self) -> None:
        """
        Test that the function raises an exception when the key is not present
        in dictionary and default value doesn't match correct expected type.
        """
        # Prepare inputs.
        dict_ = {"key0": "value0", "key1": "value1"}
        # Run.
        with self.assertRaises(AssertionError) as e:
            hdict.typed_get(
                dict_, "key2", default_value="dft_value", expected_type=int)
        # Check output.
        actual_exp = str(e.exception)
        expected_exp = r"""
        * Failed assertion *
        Instance of 'dft_value' is '<class 'str'>' instead of '<class 'int'>'
        """
        self.assert_equal(actual_exp, expected_exp, fuzzy_match=True)


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
        self.assertIsInstance(actual, str)
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
        Test that the function raises an exception with key of non-string type.
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
