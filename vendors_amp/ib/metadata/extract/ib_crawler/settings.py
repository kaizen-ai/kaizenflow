import pathlib


BOT_NAME = 'ib_crawler'

SPIDER_MODULES = ['ib_crawler.spiders']
NEWSPIDER_MODULE = 'ib_crawler.spiders'

CONCURRENT_REQUESTS_PER_IP = 10
ROBOTSTXT_OBEY = False

EXCHANGE_FNAME = "exchanges.csv"
SYMBOLS_FNAME = "symbols.csv"

ITEM_PIPELINES = {
    "ib_crawler.pipelines.ExchangeUniquePipeline": 100,
    "ib_crawler.pipelines.CSVPipeline": 200,
}