import datetime
import json
import os


async def messages_to_json(channel_link: str, output_path: str, client: object):
    """
    Download full message history to json file.
    Add date and time suffix to json file name.
    All media and telegram-specific message features are removed to make json
    dump possible.
    1 post == 1 dictionary, dictionaries are separated by newlines

    Requires an opened connection to telegram

    :param channel_link:
    :param output_path:
    :param client: telegram client with opened connection
    """
    # TODO(stas): use os.path.basename()
    name = channel_link.split("/")[-1]
    path = os.path.join(output_path, name)
    entity = await client.get_input_entity(channel_link)
    # TODO(stas): use amp.date_.get_timestamp()
    output_file_path = path + datetime.datetime.utcnow().strftime(
        "_%Y_%m_%d_%H_%M_%S.json"
    )
    # logging.info('downloading and saving' + output_file_path)

    async for message in client.iter_messages(entity):
        message_dict = message.to_dict()
        # TODO(gp): Why not str(...)?
        message_dict["date"] = message_dict["date"].__str__()
        try:
            message_dict["edit_date"] = message_dict["edit_date"].__str__()
            # TODO(stas): use narrower exception.
        except:
            pass
        try:
            message_dict["fwd_from"]["date"] = message_dict["fwd_from"][
                "date"
            ].__str__()
        except:
            pass
        try:
            del message_dict["media"]
        except:
            pass
        try:
            del message_dict["reply_markup"]
        except:
            pass
        try:
            del message_dict["action"]
        except:
            pass
        data = json.dumps(message_dict, ensure_ascii=False, skipkeys=True)
        # TODO(stats): use helpers.io_.to_file()
        with open(output_file_path, "a") as f:
            f.write(data)
            f.write("\n")
