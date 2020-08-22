import json
import logging
import os
import os.path
import re
import sys
from logging import Formatter, Handler

import ipykernel
import requests

# Alternative that works for both Python 2 and 3:
from requests.compat import urljoin

import helpers.telegram_notify.config as tg_config

try:  # Python 3 (see Edit2 below for why this may not work in Python 2)
    from notebook.notebookapp import list_running_servers
except ImportError:  # Python 2
    import warnings
    from IPython.utils.shimmodule import ShimWarning

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=ShimWarning)
        from IPython.html.notebookapp import list_running_servers

_LOG = logging.getLogger(__name__)

_TOKEN, _CHAT_ID = tg_config.get_info()


def _get_launcher_name():
    """
    Return the name of jupyter notebook or path to python file you are running.
    """
    launcher = sys.argv[0]
    if os.path.basename(launcher) == "ipykernel_launcher.py":
        kernel_id = re.search(
            "kernel-(.*).json", ipykernel.connect.get_connection_file()
        ).group(1)
        servers = list_running_servers()
        for ss in servers:
            response = requests.get(
                urljoin(ss["url"], "api/sessions"),
                params={"token": ss.get("token", "")},
            )
            for nn in json.loads(response.text):
                if nn["kernel"]["id"] == kernel_id:
                    relative_path = nn["notebook"]["path"]
                    return os.path.basename(relative_path)
    return launcher


# #############################################################################
# Send message
# #############################################################################


class TelegramNotify:
    """
    Sends notifications.
    """

    def __init__(self):
        self.launcher_name = _get_launcher_name()
        self.token = _TOKEN
        self.chat_id = _CHAT_ID

    @staticmethod
    def _send(text, token=_TOKEN, chat_id=_CHAT_ID):
        if chat_id is None or token is None:
            _LOG.warning(
                "Not sending notifications. To send notifications, both "
                "`chat_id` and `token` need to be specified. Go to README.md"
                "for more information."
            )
            return None
        else:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
            return requests.post(
                "https://api.telegram.org/bot{token}/sendMessage".format(
                    token=token
                ),
                data=payload,
            ).content

    def notify(self, message):
        msg = "<pre>{notebook_name}</pre>: {message}".format(
            notebook_name=self.launcher_name, message=message
        )
        self._send(msg, self.token, self.chat_id)


# #############################################################################
# Send notifications using logging
# #############################################################################


class _RequestsHandler(Handler):
    def emit(self, record):
        log_entry = self.format(record)
        payload = {"chat_id": _CHAT_ID, "text": log_entry, "parse_mode": "HTML"}
        return requests.post(
            "https://api.telegram.org/bot{token}/sendMessage".format(
                token=_TOKEN
            ),
            data=payload,
        ).content


class _LOGstashFormatter(Formatter):
    def __init__(self):
        super(_LOGstashFormatter, self).__init__()

    def format(self, record):
        launcher_name = _get_launcher_name()
        return "<pre>{notebook_name}</pre>: {message}".format(
            message=record.msg, notebook_name=launcher_name
        )


def init_tglogger(log_level=logging.DEBUG):
    _tg_log = logging.getLogger("telegram_notify")
    _tg_log.setLevel(log_level)
    handler = _RequestsHandler()
    formatter = _LOGstashFormatter()
    handler.setFormatter(formatter)
    _tg_log.handlers = [handler]
