#!/usr/bin/env python

import argparse
import json
import logging

import requests

from infra.helpers.telegram_notify.config import NOTIFY_JUPYTER_TOKEN
from infra.helpers.telegram_notify.telegram_notify import TelegramNotify

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)


def _get_updates_dict(token):
    updates_cont = requests.post(
        "https://api.telegram.org/bot{token}/getUpdates".format(token=token)
    ).content
    updates_dict = json.loads(updates_cont)
    assert updates_dict["ok"], updates_dict
    return updates_dict


def _get_username_id(updates_dict):
    return {
        result["message"]["from"]["username"]: result["message"]["from"]["id"]
        for result in updates_dict["result"]
    }


def _get_chat_id_updates_dict(username, updates_dict):
    username_id = _get_username_id(updates_dict)
    assert username in username_id.keys(), (
        "Either the username is wrong or you"
        " have not sent a message to the bot yet"
    )
    return username_id[username]


def send_chat_id(token, username):
    updates_dict = _get_updates_dict(token)
    chat_id = _get_chat_id_updates_dict(username, updates_dict)
    TelegramNotify._send(
        text="Your chat id is: %s" % chat_id, token=token, chat_id=chat_id
    )
    return chat_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--username", required=True, action="store", type=str)
    parser.add_argument("--token", required=False, action="store", type=str)
    args = parser.parse_args()
    username = args.username
    if args.token:
        token = args.token
    else:
        _log.info("Using default token for NotifyJupyterBot.")
        token = NOTIFY_JUPYTER_TOKEN
    chat_id = send_chat_id(token, username)
    print("Your chat id is: %s" % chat_id)
