import scrapy


class ExchangeItem(scrapy.Item):
    country = scrapy.Field()
    market = scrapy.Field()
    link = scrapy.Field()
    products = scrapy.Field()
    hours = scrapy.Field()


class ProductItem(scrapy.Item):
    category = scrapy.Field()
    title = scrapy.Field()
    ib_symbol = scrapy.Field()
    symbol = scrapy.Field()
    currency = scrapy.Field()

