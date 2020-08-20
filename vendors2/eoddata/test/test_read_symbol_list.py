import os

import helpers.unit_test as hut
import vendors2.eoddata.metadata.load as load
import vendors2.eoddata.metadata.types as mtypes


class Test_read_symbols_from_file(hut.TestCase):
    def test1(self) -> None:
        """Confirm it reads the symbol, and the exchange code is added to the
        symbol code."""
        symbol_code = "AAMC"
        symbol_name = "Altisource Asset"
        input_f = self.get_input_dir() + "/test.csv"
        exchange_code, _ = os.path.basename(input_f).split(".")

        actual = load.MetadataLoader().read_symbols_from_file(file_=input_f)

        expected = [
            mtypes.Symbol(
                Code=f"{exchange_code}:{symbol_code}",
                Name=symbol_name,
                LongName=symbol_name,
            )
        ]
        self.assertListEqual(actual, expected)
