# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import logging
import os

# %%
import telethon
# %%
# %%
from telethon.sync import TelegramClient

# %load_ext autoreload
# %autoreload 2


# %%
_log = logging.getLogger()
_log.setLevel(logging.INFO)

# %%
telethon.__version__

# %%
# These example values won't work. You must get your own api_id and
# api_hash from https://my.telegram.org, under API Development.
api_id = 942018

api_hash = '76c4ca09e06b137d3b73b209a9509790'
channel_link = 'https://t.me/markettwits'

# %%
client = TelegramClient('name14', api_id, api_hash)

# %%
await client.start()

# %%
await client.connect()

# %%
await client.get_entity(channel_link)

# %%
entity = await client.get_input_entity(channel_link)

# %%
print(entity.channel_id)

# %% [markdown]
# # finkrolik

# %%
channel_link = 'https://t.me/finkrolik'
await messages_to_json(channel_link, '/data/telegram/test', client)

# %% [markdown]
# # markettwits

# %%
channel_link = 'https://t.me/markettwits'
await messages_to_json(channel_link, '/data/telegram/test', client)

# %% [markdown]
# # russianmacro

# %%
channel_link = 'https://t.me/russianmacro'
await messages_to_json(channel_link, '/data/telegram/test', client)

# %% [markdown]
# # download Max list of channels

# %%
# except 'https://t.me/markettwits', 'https://t.me/finkrolik'
Max_list = ['https://t.me/rationalnumbers',
            'https://t.me/MarketOverview',
            'https://t.me/societe_financiers',
            'https://t.me/newBablishko',
            'https://t.me/trehlitrovayabanka',
            'https://t.me/avenuenews',
            'https://t.me/dohod',
            'https://t.me/finside',
            'https://t.me/VipCoinexhangePump',
            'https://t.me/lemonfortea',
            'https://t.me/fondu_10',
            'https://t.me/smfanton',
            'https://t.me/rothschild_son',
            'https://t.me/finpol',
            'https://t.me/FINASCOP',
            'https://t.me/Bablopobezhdaetzlo',
            'https://t.me/rbc_news',
            'https://t.me/blablanomika',
            'https://t.me/KotElviry',
            'https://t.me/accwhisper',
            'https://t.me/angrybonds',
            'https://t.me/Go_Investing',
            'https://t.me/tele_most_money',
            'https://t.me/LHfinance',
            'https://t.me/BizLike',
            'https://t.me/probonds',
            'https://t.me/gazmyaso',
            'https://t.me/finance_global',
            'https://t.me/DMTraders',
            'https://t.me/safe_money',
            'https://t.me/themovchans',
            'https://t.me/riskovik',
            'https://t.me/zloyinvestor',
            'https://t.me/divonline']

# %%

# %%
for link in Max_list:
    await messages_to_json(link, '/data/telegram/test', client)

# %%
os.listdir('/data/telegram/test')

# %%
