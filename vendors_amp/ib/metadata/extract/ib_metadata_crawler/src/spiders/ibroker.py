import csv

import scrapy
import scrapy.http as http
import scrapy.linkextractors as le
import scrapy.loader as ldr
import scrapy.loader.processors as pr
import scrapy.signals as sig

import ib_crawler.items as it


class ExchangeLoader(ldr.ItemLoader):
    default_output_processor = pr.TakeFirst()

    products_in = pr.Join(",")
    hours_in = pr.Join(" ")


class SymbolLoader(ldr.ItemLoader):
    default_output_processor = pr.TakeFirst()


class IbrokerSpider(scrapy.Spider):
    name = "ibroker"
    allowed_domains = ["interactivebrokers.com"]
    start_urls = ["https://ndcdyn.interactivebrokers.com/en/index.php?f=1562"]
    exchange_header = ["region", "country", "market", "link", "products", "hours"]
    symbols_header = [
        "market",
        "product",
        "s_title",
        "ib_symbol",
        "symbol",
        "currency",
    ]

    lx_regions = le.LinkExtractor(restrict_css="#toptabs ul.ui-tabs-nav")
    lx_exchanges = le.LinkExtractor(restrict_css="#exchange-listings table")
    lx_products = le.LinkExtractor(
        restrict_css="#exchange-products div.row div.btn-selectors",
    )
    lx_pagination = le.LinkExtractor(
        restrict_css="#exchange-products ul.pagination li:not(li.disabled)"
    )

    @classmethod
    def from_crawler(cls, crawler: scrapy.Spider, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.init_feed, signal=sig.engine_started)
        return spider

    def init_feed(self):
        with open(self.settings["EXCHANGE_FNAME"], "w") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(self.exchange_header)
        with open(self.settings["SYMBOLS_FNAME"], "w") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(self.symbols_header)

    def parse(self, response: http.HtmlResponse):
        for link in self.lx_regions.extract_links(response):
            self.logger.info(f"parse link: {link.url}")
            yield scrapy.Request(
                link.url,
                self.parse_region,
                cb_kwargs={"exchange_meta": {"region": link.text}},
            )

    def parse_region(
        self, response: http.HtmlResponse, exchange_meta: dict
    ) -> None:
        country = None
        table = response.css(
            "#exchange-listings div.col-xs-12 table.table tbody tr"
        )
        for t in table:
            row = t.css("td")
            if row.attrib.get("rowspan"):
                country = row.css("td")[0].css("::text").get()
                row = row.css("td")[1:]
            url = response.urljoin(row[0].css("a").attrib["href"])
            market = row[0].css("a::text").get()
            products = row[1].css("td::text").getall()
            hours = row[2].css("*::text").getall()
            exchange_meta["country"] = country
            exchange_meta["market"] = market
            exchange_meta["products"] = products
            exchange_meta["hours"] = hours
            yield scrapy.Request(
                url,
                self.parse_exchange,
                cb_kwargs={"exchange_meta": exchange_meta},
            )

    def parse_exchange(self, response: http.HtmlResponse, exchange_meta: dict):
        self.logger.debug("Start parse_exchange")
        link = (
            response.xpath(
                '//*[@id="exchange-info"]/div/div/div/div[1]/table/tbody/tr/td[2]/a/@href'
            ).get()
            or "N/A"
        )
        eldr = ExchangeLoader(item=it.ExchangeItem())
        eldr.add_value("link", link)
        eldr.add_value("region", exchange_meta["region"])
        eldr.add_value("country", exchange_meta["country"])
        eldr.add_value("market", exchange_meta["market"])
        eldr.add_value("products", exchange_meta["products"])
        eldr.add_value("hours", exchange_meta["hours"])
        yield eldr.load_item()

        if not self.lx_products.extract_links(response):
            return self.parse_symbols(response, exchange_meta["market"])
        for link in self.lx_products.extract_links(response):
            yield scrapy.Request(
                link.url,
                self.parse_symbols,
                cb_kwargs={"market": exchange_meta["market"]},
            )

    def parse_symbols(self, response: http.HtmlResponse, market: str):
        self.logger.debug("Start parse_symbols")
        product = (
            response.css(
                "#exchange-products div.btn-selectors p a.btn.btn-default::text"
            ).get()
            or "N/A"
        )
        symbol_title = response.css(
            "#exchange-products div.col-xs-12.col-sm-12 table.table tbody tr td a::text"
        )
        symbols = response.css("#exchange-products div.col-xs-12 tbody tr")
        for row in symbols:
            s_data = row.css("td::text").getall()
            if len(s_data) < 4:
                symbol_title = row.css("td a::text").get()
                s_data.insert(1, symbol_title)
            item = SymbolLoader(item=it.SymbolItem())
            item.add_value("market", market)
            item.add_value("product", product)
            item.add_value("s_title", s_data[1])
            item.add_value("ib_symbol", s_data[0])
            item.add_value("symbol", s_data[2])
            item.add_value("currency", s_data[3])
            yield item.load_item()

        next_page = self.lx_pagination.extract_links(response)[-1]
        if ">" in next_page.text:
            yield scrapy.Request(
                next_page.url, self.parse_symbols, cb_kwargs={"market": market},
            )
