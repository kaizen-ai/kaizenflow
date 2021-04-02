#!/usr/bin/env python

"""
# Make an API call every 10 seconds to get the history of symbol `MSFT`

> check_realtime_feed.py -u $KIBOT_USERNAME -p $KIBOT_PASSWORD
"""

import logging
import time

import requests

import instrument_master.kibot.base.command as vkbcom
import instrument_master.kibot.data.config as vkdcon

_LOG = logging.getLogger(__name__)


# #############################################################################


# TODO(*): -> CheckRealtimeFeedCommand
class CheckReadtimeFeedCommand(vkbcom.KibotCommand):
    def __init__(self) -> None:
        super().__init__(
            docstring=__doc__, requires_auth=True, requires_api_login=True
        )

    def customize_run(self) -> int:
        # Download file.
        while True:
            response = requests.get(
                url=vkdcon.API_ENDPOINT,
                params=dict(
                    action="history", symbol="MSFT", interval="1", period="2"
                ),
            )

            print(f"received {len(response.text.split())} data points.")
            time.sleep(10)
        return 0


if __name__ == "__main__":
    CheckReadtimeFeedCommand().run()
