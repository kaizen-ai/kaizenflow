import argparse
import asyncio
import logging
import os

import requests
import telegram

import helpers.hdbg as hdbg
import helpers.hparser as hparser

bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
_LOG = logging.getLogger(__name__)
_TELEGRAM_API = "https://api.telegram.org/bot"


def _get_invite_link(group_id: str) -> str:
    """
    Create a invate link of the Telegram group.
    """
    response = requests.get(
        f"{_TELEGRAM_API}{bot_token}/exportChatInviteLink?chat_id={group_id}"
    )
    status_code = response.status_code
    if status_code == 200:
        invite_link = response.json().get("result")
        return invite_link
    else:
        _LOG.debug(
            "Error retrieving permission level for %s. Status code: %s",
            group_id,
            status_code,
        )
    return None


async def _invite_collaborator(
    bot: telegram.Bot, user_id: str, group_id: str
) -> None:
    """
    Invite a collaborator to Telegram.
    """
    link = _get_invite_link(group_id)
    hdbg.dassert_is_not(link, None)
    message = await bot.send_message(chat_id=user_id, text=link)
    hdbg.dassert_is_not(message, None)


async def _remove_collaborator(
    bot: telegram.Bot, user_id: str, group_id: str
) -> None:
    """
    Remove a collaborator from Telegram.
    """
    await bot.banChatMember(group_id, user_id)
    await bot.unbanChatMember(group_id, user_id)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action",
        type=str,
        choices=["add", "remove", "unban"],
        required=True,
        help="Action to perform: add or remove",
    )
    parser.add_argument(
        "--username", type=str, required=True, help="Username of the user"
    )
    parser.add_argument(
        "--groupid",
        type=str,
        required=True,
        help="Id of the group to add to",
    )
    hparser.add_verbosity_arg(parser)
    return parser


async def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    action = args.action
    username = args.username
    groupid = args.groupid
    bot = telegram.Bot(token=bot_token)
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    if action == "add":
        await _invite_collaborator(bot, username, groupid)
    elif action == "remove":
        await _remove_collaborator(bot, username, groupid)
    else:
        raise ValueError("Invalid action ='%s'" % action)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main(_parse()))
