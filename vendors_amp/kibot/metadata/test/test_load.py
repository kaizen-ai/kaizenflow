import os

import pytest

import helpers.io_ as hio
import helpers.unit_test as hut
import vendors_amp.kibot.metadata.load as vkmloa
import vendors_amp.kibot.metadata.types as vkmtyp


class TestTickerListLoader(hut.TestCase):
    def test_parsing_logic(self) -> None:
        lines = hio.from_file(
            file_name=os.path.join(self.get_input_dir(), "test.txt")
        ).split("\n")

        loader = vkmloa.TickerListsLoader()
        listed_tickers, delisted_tickers = loader._parse_lines(lines=lines)

        self.assertEqual(
            listed_tickers,
            [
                vkmtyp.Ticker(
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
                vkmtyp.Ticker(
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

    @pytest.mark.skip("Disabled waiting for PTask4139")
    def test_real_call(self) -> None:
        tickers = vkmloa.TickerListsLoader().get(ticker_list="dow_30_intraday")

        self.assertEqual(len(tickers), 43)

        self.assertEqual(
            tickers[0],
            vkmtyp.Ticker(
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
    @pytest.mark.skip("Disabled waiting for PTask4139")
    def test_real_call(self) -> None:
        adjustments = vkmloa.AdjustmentsLoader().load(symbol="SPTN")

        self.assertEqual(len(adjustments), 58)

        self.assertEqual(
            adjustments[0],
            vkmtyp.Adjustment(
                Date="2/27/2006",
                Symbol="SPTN",
                Company="SpartanNash Company",
                Action=0.05,
                Description="Dividend",
                EventDate="2/27/2006",
            ),
        )
