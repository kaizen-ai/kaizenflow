from typing import DefaultDict, List
import collections as col

import scrapy
import scrapy.http as http
import scrapy.loader as ldr
import scrapy.loader.processors as pr
import scrapy.linkextractors as le

import extract.ib_crawler.items as it


class ExchangeLoader(ldr.ItemLoader):
    default_output_processor = pr.Identity()

    products_in = pr.Join(",")
    hours_in = pr.Join(" ")


class IbrokerSpider(scrapy.Spider):
    name = 'ibroker'
    allowed_domains = ['interactivebrokers.com']
    start_urls = ['https://ndcdyn.interactivebrokers.com/en/index.php?f=1562']

    lx_regions = le.LinkExtractor(
        restrict_css="#overview div.container div.row"
    )
    lx_exchanges = le.LinkExtractor(
        restrict_css="#exchange-listings table"
    )
    lx_products = le.LinkExtractor(
        restrict_css='#exchange-products div.row div.btn-selectors',
    )

    def parse(self, response: http.HtmlResponse):
        for link in self.lx_regions.extract_links(response):
            yield scrapy.Request(
                link.url,
                self.parse_region,
                cb_kwargs={"region": link.text},
            )

    def parse_region(self, response: http.HtmlResponse, region: str):
        for link in self.lx_exchanges.extract_links(response):
            loader = ExchangeLoader(item=it.ExchangeItem)
            yield scrapy.Request(
                link.url,
                self.parse_exchanges, cb_kwargs={"item": loader},
            )

    def parse_exchanges(self, response: http.HtmlResponse, item: ExchangeLoader):
        # header = response.css(
        #     '#exchange-listings div.col-xs-12 table.table thead tr th::text'
        # ).getall()
        country = None
        table = response.css(
            '#exchange-listings div.col-xs-12 table.table tbody tr'
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
            item.add_value("country", country)
            item.add_value("market", market)
            item.add_value("products", products)
            item.add_value("hours", hours)
            yield scrapy.Request(
                url, self.parse_category, cb_kwargs={"item": item}
            )

    def parse_category(self, response: http.HtmlResponse, item: ExchangeLoader):
        link = response.xpath(
            '//*[@id="exchange-info"]/div/div/div/div[1]/table/tbody/tr/td[2]/a/@href'
        ).get() or "N/A"
        item.add_value("link", link)
        if not self.lx_products.extract_links(response):
            return self.parse_product(response, item)
        for link in self.lx_products.extract_links(response):
            yield scrapy.Request(
                link.url, self.parse_product, cb_kwargs={"item": item}
            )

    def parse_product(self, response: http.HtmlResponse, item: ExchangeLoader):
        product = response.css(
            '#exchange-products div.btn-selectors p a.btn.btn-default::text'
        ).get() or "N/A"
        # hours = response.xpath(
        #     '//*[@id="exchange-info"]/div/div/div/div[1]/table/tbody/tr/td[1]/text()'
        # ).get() or "N/A"
        symbol_title = c
        symbols = self._parse_symbols(
            response.css('#exchange-products div.col-xs-12 tbody tr')
        )

    def _parse_symbols(self, symbol_tb: List[scrapy.Selector]):
        symbols = list()
        for row in symbol_tb:
            data = row.css('td::text').getall()
            if len(data) < 4:
                product = row.css('td a::text')
                data.insert(1, product)
            symbols.append(data)


