import pandas as pd
from unittest.mock import patch

import helpers.unit_test as hut
import vendors2.kibot.metadata.load.contract_symbol_mapping as csm


def mock_test_contract_symbol_mapping_init(self: csm.ContractSymbolMapping):
    md = {
        'Kibot_symbol': ['test'],
        'Exchange_abbreviation': ['tt'],
        'Exchange_symbol': ['TT']
     }
    self._metadata = pd.DataFrame.from_dict(md)


class TestContractSymbolMapper(hut.TestCase):
    @staticmethod
    def _mock_csm():
        return patch.object(csm.ContractSymbolMapping, '__init__', mock_test_contract_symbol_mapping_init)

    def test_get_contract1(self):
        """Valid input returns a valid output."""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = 'test'
            exp = 'tt:TT'
            self.assertEqual(exp, cls.get_contract(inp))

    def test_get_contract2(self):
        """No input returns null"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = ''
            exp = None
            self.assertEqual(exp, cls.get_contract(inp))

    def test_get_contract3(self):
        """Invalid input returns null"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = 'test2'
            exp = None
            self.assertEqual(exp, cls.get_contract(inp))

    def test_get_kibot_symbol1(self):
        """Valid input returns valid output"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = 'tt:TT'
            exp = 'test'
            self.assertEqual(exp, cls.get_kibot_symbol(inp))

    def test_get_kibot_symbol2(self):
        """No input raises an error"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = ''
            with self.assertRaises(AssertionError):
                cls.get_kibot_symbol(inp)

    def test_get_kibot_symbol3(self):
        """Incorrect input raises an error"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = 'test2'
            with self.assertRaises(AssertionError):
                cls.get_kibot_symbol(inp)

    def test_get_kibot_symbol4(self):
        """Invalid input raises an error"""
        with self._mock_csm():
            cls = csm.ContractSymbolMapping()
            inp = 'test:test'
            exp = None
            self.assertEqual(exp, cls.get_kibot_symbol(inp))
