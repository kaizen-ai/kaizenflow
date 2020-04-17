import logging

import pytest

import helpers.dict as dct
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_get_nested_dict_iterator(hut.TestCase):
    def test_flat_dict1(self):
        dict_ = {"key0": "value0", "key1": "value1"}
        actual_result = list(dct.get_nested_dict_iterator(dict_))
        expected_result = [(["key0"], "value0"), (["key1"], "value1")]
        self.assertListEqual(actual_result, expected_result)

    def test_nested_dict1(self):
        dict_ = {
            "key0": {"key00": "value00", "key01": "value01"},
            "key1": "value1",
        }
        actual_result = list(dct.get_nested_dict_iterator(dict_))
        expected_result = [
            (["key0", "key00"], "value00"),
            (["key0", "key01"], "value01"),
            (["key1"], "value1"),
        ]
        self.assertListEqual(actual_result, expected_result)

    def test_nested_dict2(self):
        dict_ = {"key0": {"key00": {"key000": "value000"}}, "key1": "value1"}
        actual_result = list(dct.get_nested_dict_iterator(dict_))
        expected_result = [
            (["key0", "key00", "key000"], "value000"),
            (["key1"], "value1"),
        ]
        self.assertListEqual(actual_result, expected_result)

    def test_flat_dict_with_none1(self):
        dict_ = {"key0": "value0", "key1": None}
        actual_result = list(dct.get_nested_dict_iterator(dict_))
        expected_result = [(["key0"], "value0"), (["key1"], None)]
        self.assertListEqual(actual_result, expected_result)

    def test_nested_dict_with_none1(self):
        dict_ = {"key0": {"key00": None}, "key1": "value1"}
        actual_result = list(dct.get_nested_dict_iterator(dict_))
        expected_result = [(["key0", "key00"], None), (["key1"], "value1")]
        self.assertListEqual(actual_result, expected_result)

    @pytest.mark.skip
    def test_flat_dict_with_empty_subdict1(self):
        dict_ = {"key0": {}, "key1": "value1"}
        actual_result = list(
            dct.get_nested_dict_iterator(dict_)
        )  # [(['key1'], 'value1')]
        expected_result = [(["key0"], {}), (["key1"], "value1")]
        self.assertListEqual(actual_result, expected_result)

    @pytest.mark.skip
    def test_nested_dict_with_empty_subdict1(self):
        dict_ = {"key0": {"key00": {}}, "key1": "value1"}
        actual_result = list(
            dct.get_nested_dict_iterator(dict_)
        )  # [(['key1'], 'value1')]
        expected_result = [(["key0, key00"], {}), (["key1"], "value1")]
        self.assertListEqual(actual_result, expected_result)
