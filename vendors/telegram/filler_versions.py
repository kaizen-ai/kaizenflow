from .base_classes import TelegramJsonFiller
from twitter_data import data_config as data_cfg
from data import data_config

TELEGRAM_FILLERS = {
    'TELEGRAM_MARKET_TWEETS_V1': {
        'class': TelegramJsonFiller,
        'settings': {'db_config': data_cfg.ACCOUNTS_DB_CONFIG,
                     'db_name': data_cfg.ACCOUNTS_DB_NAME,
                     'collection_name': 'telegram_markettwits_v1',
                     'paranoid': True,
                     },
        'optional': {'src_data': data_config.TASK271_MARKETTWITS}
    },
    'TELEGRAM_FINKROLIK_V1': {
        'class': TelegramJsonFiller,
        'settings': {'db_config': data_cfg.ACCOUNTS_DB_CONFIG,
                     'db_name': data_cfg.ACCOUNTS_DB_NAME,
                     'collection_name': 'telegram_finkrolik_v1',
                     'paranoid': True,
                     },
        'optional': {'src_data': data_config.TASK271_FINKROLIK}
    }
}

IMPORT_FILLER = {
    'human_name': 'Telegram fillers',
    'fillers': TELEGRAM_FILLERS
}
