# telegram-notify

Send notifications via telegram.

## Getting Started

### Getting token and chat id
Start messaging with either `https://t.me/NotifyJupyterBot` or with your bot by
sending it `/start` message. Activate your environment, then from `infra/` run
```bash
python helpers/telegram_notify/get_chat_id.py --username <your telegram username>
```
Specify `--token <your bot token>` if you are using a custom bot.

This will display a message in your terminal with your chat id and also send a
message with it through the bot.

### Modifying config for your token and chat id

Go to `config.py`. There you will need to insert the following:
```python
elif user == <your local user name>:
    TELEGRAM_TOKEN = <bot token>
    TELEGRAM_CHAT_ID = <chat id>
```
You can push the modified file. To do this, you need to go to
`commodity_project/infra`, otherwise your changes will not be saved.

## How to use
There are two ways of using this bot:
- send notifications through `TelegramNotify` class
- send notifications using logging

In both cases the bot will send you something like this:
`<program name>: <your_message>`

### TelegramNotify

```python
import infra.helpers.telegram_notify.telegram_notify as tg

tgn = tg.TelegramNotify()
tgn.notify('test message')
```

### Logging
```python
import logging
import infra.helpers.telegram_notify.telegram_notify as tg

_tg_log = logging.getLogger('telegram_notify')
_tg_log.setLevel(logging.INFO)
tg.init_tglogger()

_tg_log.info('test message')
```
