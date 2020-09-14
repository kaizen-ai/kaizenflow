#!/usr/bin/env python

"""# Make an API call every 10 seconds to get the history of symbol `MSFT`

> check_realtime_feed.py -u $P1_KIBOT_USERNAME -p $P1_KIBOT_PASSWORD
"""

import logging
import time

import requests

import vendors2.kibot.base.command as command
import vendors2.kibot.data.config as config

_LOG = logging.getLogger(__name__)


# #############################################################################


class CheckReadtimeFeedCommand(command.KibotCommand):
    REQUIRES_AUTH = True
    REQUIRES_API_LOGIN = True

    def customize_run(self) -> int:
        # Download file.
        while True:
            response = requests.get(
                url=config.API_ENDPOINT,
                params=dict(
                    action="history", symbol="MSFT", interval="1", period="2"
                ),
            )

            print(f"received {len(response.text.split())} data points.")
            time.sleep(10)
        return 0


if __name__ == "__main__":
    CheckReadtimeFeedCommand().run()
