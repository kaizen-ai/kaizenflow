import os
from typing import List, Tuple

import smart_open

import vendors2.kibot.metadata.config as config
import vendors2.kibot.metadata.types as types


class TickerListsLoader:
    @staticmethod
    def _get_lines(s3_path: str) -> List[str]:
        with smart_open.open(s3_path, "r") as fh:
            return fh.readlines()

    def _parse_lines(
        self, lines: List[str]
    ) -> Tuple[List[types.Ticker], List[types.Ticker]]:
        """Get a list of listed & delisted tickers from lines."""
        listed_tickers: List[types.Ticker] = []
        delisted_tickers: List[types.Ticker] = []

        listed_found = False
        header_skipped = False
        delisted_found = False
        for line in lines:
            if not line.strip():
                # Skip empty lines.
                continue

            if line.strip() == "Listed:":
                listed_found = True
                continue
            if line.strip() == "Delisted:":
                delisted_found = True
                continue

            if listed_found and not delisted_found:
                if not header_skipped:
                    header_skipped = True
                    continue

                listed_tickers.append(self._get_ticker_from_line(line))
            elif delisted_found:
                delisted_tickers.append(self._get_ticker_from_line(line))

        return listed_tickers, delisted_tickers

    def get(self, ticker_list: str, listed: bool = True) -> List[types.Ticker]:
        s3_path = os.path.join(
            config.S3_PREFIX, config.TICKER_LISTS_SUB_DIR, f"{ticker_list}.txt",
        )

        lines = self._get_lines(s3_path=s3_path)
        listed_tickers, delisted_tickers = self._parse_lines(lines=lines)
        return listed_tickers if listed else delisted_tickers

    @staticmethod
    def _get_ticker_from_line(line: str) -> types.Ticker:
        """Get a ticker from a line.

        Example line:
        #       Symbol  StartDate       Size(MB)        Description     Exchange        Industry        Sector
        1       AA      4/27/2007       68      "Alcoa Corporation"     NYSE    "Aluminum"      "Basic Industries"
        """
        args = line.split("\t")
        # Remove new line from last element. Note: if we strip before splitting, the tab
        # delimiters would be removed as well if a column is empty.
        args[-1] = args[-1].strip()
        # Skip index col.
        args = args[1:]
        ret = types.Ticker(*args)
        return ret
