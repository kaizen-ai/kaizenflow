import os
from typing import List

import smart_open

import vendors2.kibot.metadata.config as config
import vendors2.kibot.metadata.types as types


class TickerListsLoader:
    def load(self, ticker_list: str, listed: bool = True) -> List[types.Ticker]:
        file_path = os.path.join(
            config.S3_PREFIX, config.TICKER_LISTS_SUB_DIR, f"{ticker_list}.txt",
        )

        listed_tickers: List[types.Ticker] = []
        delisted_tickers: List[types.Ticker] = []

        listed_found = False
        header_skipped = False
        delisted_found = False
        with smart_open.open(file_path, "r") as fh:
            for line in fh.readlines():
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

        return listed_tickers if listed else delisted_tickers

    @staticmethod
    def _get_ticker_from_line(line: str) -> types.Ticker:
        args = line.split("\t")
        # Remove new line from last element. Note: if we strip before splitting, the tab
        # delimiters would be removed as well if a column is empty.
        args[-1] = args[-1].strip()
        # Skip index col.
        args = args[1:]
        ret = types.Ticker(*args)
        return ret
