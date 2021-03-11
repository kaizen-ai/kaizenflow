import scrapy


class ExchangeItem(scrapy.Item):
    region = scrapy.Field()
    country = scrapy.Field()
    market = scrapy.Field()
    link = scrapy.Field()
    products = (
        scrapy.Field()
    )  # TODO: implement postprocessor for list -> ",".join method
    hours = scrapy.Field()  # TODO: implement postprocessor for list -> " ".join()


class SymbolItem(scrapy.Item):
    market = scrapy.Field()
    product = scrapy.Field()
    s_title = scrapy.Field()
    ib_symbol = scrapy.Field()
    symbol = scrapy.Field()
    currency = scrapy.Field()
