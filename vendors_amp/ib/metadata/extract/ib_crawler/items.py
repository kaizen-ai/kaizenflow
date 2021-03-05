from urllib.parse import SplitResult
import scrapy


class ExchangeItem(scrapy.Item):
    region = scrapy.Field()
    country = scrapy.Field()
    market = scrapy.Field()
    link = scrapy.Field()
    products = scrapy.Field()   # TODO: implement postprocessor for list -> ",".join method
    product = scrapy.Field()
    hours = scrapy.Field()  # TODO: implement postprocessor for list -> " ".join()
    ib_symbol = scrapy.Field()
    symbol_title = scrapy.Field()
    symbol = scrapy.Field()
    currency = scrapy.Field()

