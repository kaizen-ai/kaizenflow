import os

import pytest

import helpers.unit_test as hut
import vendors2.kibot.metadata.load as load
import vendors2.kibot.metadata.types as types


class TestTickerListLoader(hut.TestCase):
    def test_parsing_logic(self) -> None:
        with open(os.path.join(self.get_input_dir(), "test.txt"), "r") as fh:
            lines = fh.readlines()

        loader = load.TickerListsLoader()
        listed_tickers, delisted_tickers = loader._parse_lines(lines=lines)

        assert listed_tickers == [
            types.Ticker(
                Symbol="AA",
                StartDate="4/27/2007",
                Size="68",
                Description='"Alcoa Corporation"',
                Exchange="NYSE",
                Industry='"Aluminum"',
                Sector='"Basic Industries"',
            )
        ]

        assert delisted_tickers == [
            types.Ticker(
                Symbol="XOM",
                StartDate="12/1/1999",
                Size="102",
                Description='"Exxon Mobil Corporation"',
                Exchange="NYSE",
                Industry='"Integrated oil Companies"',
                Sector='"Energy"',
            )
        ]

    @pytest.mark.xfail(reason="Requires S3 access")
    def test_real_call(self) -> None:
        tickers = load.TickerListsLoader().get(ticker_list="dow_30_intraday")
        assert len(tickers) == 43

        assert tickers[0] == types.Ticker(
            Symbol="AA",
            StartDate="4/27/2007",
            Size="68",
            Description='"Alcoa Corporation"',
            Exchange="NYSE",
            Industry='"Aluminum"',
            Sector='"Basic Industries"',
        )
