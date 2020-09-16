import os

import pytest

import helpers.io_ as io_
import helpers.unit_test as hut
import vendors2.kibot.metadata.load as load
import vendors2.kibot.metadata.types as types


class TestTickerListLoader(hut.TestCase):
    def test_parsing_logic(self) -> None:
        lines = io_.from_file(
            file_name=os.path.join(self.get_input_dir(), "test.txt")
        ).split("\n")

        loader = load.TickerListsLoader()
        listed_tickers, delisted_tickers = loader._parse_lines(lines=lines)

        self.assertEqual(
            listed_tickers,
            [
                types.Ticker(
                    Symbol="AA",
                    StartDate="4/27/2007",
                    Size="68",
                    Description='"Alcoa Corporation"',
                    Exchange="NYSE",
                    Industry='"Aluminum"',
                    Sector='"Basic Industries"',
                )
            ],
        )

        self.assertEqual(
            delisted_tickers,
            [
                types.Ticker(
                    Symbol="XOM",
                    StartDate="12/1/1999",
                    Size="102",
                    Description='"Exxon Mobil Corporation"',
                    Exchange="NYSE",
                    Industry='"Integrated oil Companies"',
                    Sector='"Energy"',
                )
            ],
        )

    @pytest.mark.skip("Disabled waiting for PartTask4139")
    def test_real_call(self) -> None:
        tickers = load.TickerListsLoader().get(ticker_list="dow_30_intraday")

        self.assertEqual(len(tickers), 43)

        self.assertEqual(
            tickers[0],
            types.Ticker(
                Symbol="AA",
                StartDate="4/27/2007",
                Size="68",
                Description='"Alcoa Corporation"',
                Exchange="NYSE",
                Industry='"Aluminum"',
                Sector='"Basic Industries"',
            ),
        )


class TestAdjustmentsLoader(hut.TestCase):
    @pytest.mark.skip("Disabled waiting for PartTask4139")
    def test_real_call(self) -> None:
        adjustments = load.AdjustmentsLoader().load(symbol="SPTN")

        self.assertEqual(len(adjustments), 58)

        self.assertEqual(
            adjustments[0],
            types.Adjustment(
                Date="2/27/2006",
                Symbol="SPTN",
                Company="SpartanNash Company",
                Action=0.05,
                Description="Dividend",
                EventDate="2/27/2006",
            ),
        )
