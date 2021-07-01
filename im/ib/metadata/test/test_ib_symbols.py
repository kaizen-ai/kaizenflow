import os

import pytest

import helpers.unit_test as hut
import im.common.data.types as icdtyp
import im.common.metadata.symbols as icmsym
import im.ib.data.config as iidcon
import im.ib.data.load.ib_file_path_generator as iidlib
import im.ib.metadata.ib_symbols as iimibs


class TestIbSymbolUniverse(hut.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Disable the chatty modules when debugging with DEBUG verbosity. We need to
        # disable the modules after they have been imported.
        # import helpers.dbg as dbg
        # dbg.shutup_chatty_modules(verbose=True)
        hut.TestCase.setUpClass()

    def test_parse_symbols_file1(self) -> None:
        """
        Test parsing a file checked in the repo.
        """
        symbols_file = os.path.join(self.get_input_dir(), "test_symbols.csv")
        symbols = iimibs.IbSymbolUniverse._parse_symbols_file(symbols_file)
        # Build string to check.
        symbols_str = "\n".join([str(symbol) for symbol in symbols])
        self.check_string(symbols_str)

    @pytest.mark.slow("Parse real large file with symbols. Approx. 15 sec.")
    def test_parse_symbols_file2(self) -> None:
        """
        Test parsing the real file.
        """
        symbols_file = os.path.join(
            iidcon.S3_PREFIX, "metadata/symbols-2021-04-01-134738089177.csv"
        )
        symbols = iimibs.IbSymbolUniverse._parse_symbols_file(symbols_file)
        # Construct string to check.
        symbols_str = "Total parsed symbols: %i\n" % len(symbols)
        # Add first 5 symbols.
        symbols_str += "\n".join([str(symbol) for symbol in symbols[:5]])
        # Add last 5 symbols.
        symbols_str += "\n"
        symbols_str += "\n".join([str(symbol) for symbol in symbols[-5:]])
        self.check_string(symbols_str)

    def test_convert_df_to_row_to_symbol1(self) -> None:
        """
        Test supported stocks symbol converting.
        """
        act = iimibs.IbSymbolUniverse._convert_df_to_row_to_symbol(
            ib_ticker="AA",
            ib_exchange="New York (NYSE)",
            ib_asset_class="Stocks",
            ib_currency="USD",
        )
        exp = icmsym.Symbol(
            ticker="AA",
            exchange="NYSE",
            asset_class=icdtyp.AssetClass.Stocks,
            contract_type=None,
            currency="USD",
        )
        self.assertEqual(act, exp)

    def test_convert_df_to_row_to_symbol2(self) -> None:
        """
        Test supported futures symbol converting.
        """
        converted_symbol = iimibs.IbSymbolUniverse._convert_df_to_row_to_symbol(
            ib_ticker="ZC",
            ib_exchange="CME part (ECBOT)",
            ib_asset_class="Futures",
            ib_currency="USD",
        )
        expected_symbol = icmsym.Symbol(
            ticker="ZC",
            exchange="ECBOT",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            currency="USD",
        )
        self.assertEqual(converted_symbol, expected_symbol)

    def test_convert_df_to_row_to_symbol3(self) -> None:
        """
        Test symbol with unsupported exchange.
        """
        converted_symbol = iimibs.IbSymbolUniverse._convert_df_to_row_to_symbol(
            ib_ticker="AA",
            ib_exchange="No brackets exchange",
            ib_asset_class="Stocks",
            ib_currency="USD",
        )
        self.assertIsNone(converted_symbol)

    def test_convert_df_to_row_to_symbol4(self) -> None:
        """
        Test symbol with unsupported asset class.
        """
        converted_symbol = iimibs.IbSymbolUniverse._convert_df_to_row_to_symbol(
            ib_ticker="AA",
            ib_exchange="New York (NYSE)",
            ib_asset_class="Warrants",
            ib_currency="USD",
        )
        self.assertIsNone(converted_symbol)

    def test_extract_exchange_code_from_full_name1(self) -> None:
        """
        Test uppercase name extraction from single brackets.
        """
        extracted_exchange = (
            iimibs.IbSymbolUniverse._extract_exchange_code_from_full_name(
                "What a great (NAME)"
            )
        )
        self.assert_equal(extracted_exchange, "NAME")

    def test_extract_exchange_code_from_full_name2(self) -> None:
        """
        Test uppercase name extraction from no brackets string.
        """
        extracted_exchange = (
            iimibs.IbSymbolUniverse._extract_exchange_code_from_full_name("NAME")
        )
        self.assert_equal(extracted_exchange, "NAME")

    def test_extract_exchange_code_from_full_name3(self) -> None:
        """
        Test non-uppercase name extraction from single brackets.
        """
        extracted_exchange = (
            iimibs.IbSymbolUniverse._extract_exchange_code_from_full_name(
                "What a great (Name)"
            )
        )
        self.assertIsNone(extracted_exchange)

    def test_extract_exchange_code_from_full_name4(self) -> None:
        """
        Test non-uppercase name extraction from no brackets string.
        """
        extracted_exchange = (
            iimibs.IbSymbolUniverse._extract_exchange_code_from_full_name("Name")
        )
        self.assertIsNone(extracted_exchange)

    def test_extract_exchange_code_from_full_name5(self) -> None:
        """
        Test latest uppercase name extraction from two pairs of brackets
        string.
        """
        extracted_exchange = (
            iimibs.IbSymbolUniverse._extract_exchange_code_from_full_name(
                "One (NAME) two (NAMES)"
            )
        )
        self.assert_equal(extracted_exchange, "NAMES")

    @pytest.mark.slow("Around 15 sec.")
    def test_get_1(self) -> None:
        """
        Test that ES symbol is returned by request.
        """
        # Parse real large file with symbols.
        file_name = iidlib.IbFilePathGenerator.get_latest_symbols_file()
        ib_universe = iimibs.IbSymbolUniverse(file_name)
        matched = ib_universe.get(
            ticker="ES",
            exchange="GLOBEX",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            currency="USD",
        )
        # TODO(gp): Use the actual outcome.
        self.assertEqual(len(matched), 1)

    @pytest.mark.slow("Around 15 sec.")
    def test_get_2(self) -> None:
        """
        Test that NON_EXISTING symbol is returned by request.
        """
        # Parse real large file with symbols.
        file_name = iidlib.IbFilePathGenerator.get_latest_symbols_file()
        ib_universe = iimibs.IbSymbolUniverse(file_name)
        matched = ib_universe.get(
            ticker="NON_EXISTING",
            exchange="GLOBEX",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            currency="USD",
        )
        self.assertEqual(matched, [])

    @pytest.mark.slow("Around 15 sec.")
    def test_get_3(self) -> None:
        """
        Test that NG symbol is in downloaded list.
        """
        # Parse real large file with symbols.
        file_name = iidlib.IbFilePathGenerator.get_latest_symbols_file()
        ib_universe = iimibs.IbSymbolUniverse(file_name)
        matched = ib_universe.get(
            ticker="NG",
            exchange="NYMEX",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            currency="USD",
            is_downloaded=True,
            frequency=icdtyp.Frequency.Minutely,
            path_generator=iidlib.IbFilePathGenerator(),
        )
        # TODO(gp): Use the actual outcome.
        self.assertEqual(len(matched), 1)

    @pytest.mark.slow("Around 15 sec.")
    def test_get_4(self) -> None:
        """
        Test that NON_EXISTING symbol is not in the downloaded list.
        """
        # Parse real large file with symbols.
        file_name = iidlib.IbFilePathGenerator.get_latest_symbols_file()
        ib_universe = iimibs.IbSymbolUniverse(file_name)
        matched = ib_universe.get(
            ticker="NON_EXISTING",
            exchange="GLOBEX",
            asset_class=icdtyp.AssetClass.Futures,
            contract_type=icdtyp.ContractType.Continuous,
            currency="USD",
            is_downloaded=True,
            frequency=icdtyp.Frequency.Minutely,
            path_generator=iidlib.IbFilePathGenerator(),
        )
        self.assertEqual(matched, [])
