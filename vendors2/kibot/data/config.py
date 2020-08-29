ENDPOINT = "http://www.kibot.com/"

API_ENDPOINT = "http://api.kibot.com/"

S3_PREFIX = "s3://external-p1/kibot"

DATASETS = [
    "adjustments",
    "all_stocks_1min",
    "all_stocks_unadjusted_1min",
    "all_stocks_daily",
    "all_stocks_unadjusted_daily",
    #
    "all_etfs_1min",
    "all_etfs_unadjusted_1min",
    "all_etfs_daily",
    "all_etfs_unadjusted_daily",
    #
    "all_forex_pairs_1min",
    "all_forex_pairs_daily",
    #
    "all_futures_contracts_1min",
    "all_futures_contracts_daily",
    # TODO(gp): -> tickbidask?
    "all_futures_continuous_contracts_tick",
    "all_futures_continuous_contracts_1min",
    "all_futures_continuous_contracts_daily",
    #
    "sp_500_tickbidask",
    "sp_500_unadjusted_tickbidask",
    "sp_500_1min",
    "sp_500_unadjusted_1min",
    "sp_500_daily",
    "sp_500_unadjusted_daily",
]
