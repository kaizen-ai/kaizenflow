import os

import pytest

import helpers.hio as hio
import helpers.hunit_test as hunitest
import im_v2.kibot.metadata.load as vkmloa
import im_v2.kibot.metadata.types as imvkimety


class TestTickerListLoader(hunitest.TestCase):
    def test_parsing_logic(self) -> None:
        lines = hio.from_file(
            file_name=os.path.join(self.get_input_dir(), "test.txt")
        ).split("\n")
        loader = vkmloa.TickerListsLoader()
        listed_tickers, delisted_tickers = loader._parse_lines(lines=lines)
        self.assertEqual(
            listed_tickers,
            [
                imvkimety.Ticker(
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
                imvkimety.Ticker(
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
            imvkimety.Ticker(
                Symbol="AA",
                StartDate="4/27/2007",
                Size="68",
                Description='"Alcoa Corporation"',
                Exchange="NYSE",
                Industry='"Aluminum"',
                Sector='"Basic Industries"',
            ),
        )


class TestAdjustmentsLoader(hunitest.TestCase):
    @pytest.mark.skip("Disabled waiting for PTask4139")
    def test_real_call(self) -> None:
        adjustments = vkmloa.AdjustmentsLoader().load(symbol="SPTN")
        self.assertEqual(len(adjustments), 58)
        self.assertEqual(
            adjustments[0],
            imvkimety.Adjustment(
                Date="2/27/2006",
                Symbol="SPTN",
                Company="SpartanNash Company",
                Action=0.05,
                Description="Dividend",
                EventDate="2/27/2006",
            ),
        )
