#  TELEGRAM_TOKEN is the token of your bot. You can use @NotifyJupyterBot,
# its token is '***REMOVED***'

# TELEGRAM_CHAT_ID: To get it, start messaging with the bot. Then go to
# https://api.telegram.org/bot<TELEGRAM_TOKEN>/getUpdates and get your chat id.
# (If you are using @NotifyJupyterBot, go to
# https://api.telegram.org/bot***REMOVED***/getUpdates )

# Be sure to go to infra/ when you commit this file!

import getpass
import logging

_log = logging.getLogger(__name__)

NOTIFY_JUPYTER_TOKEN = "***REMOVED***"

user = getpass.getuser()

if user == "julia":
    TELEGRAM_TOKEN = NOTIFY_JUPYTER_TOKEN
    TELEGRAM_CHAT_ID = "35712077"
elif user == "liza":
    TELEGRAM_TOKEN = NOTIFY_JUPYTER_TOKEN
    TELEGRAM_CHAT_ID = "112840786"
else:
    TELEGRAM_TOKEN = NOTIFY_JUPYTER_TOKEN
    TELEGRAM_CHAT_ID = None
    _log.warning("User %s is not in the tg_notify_config.py" % user)
