import scrapy
import scrapy.linkextractors as le


class IbrokerSpider(scrapy.Spider):
    name = 'ibroker'
    allowed_domains = ['interactivebrokers.com']
    start_urls = ['https://ndcdyn.interactivebrokers.com/en/index.php?f=46390']

    lx_categories = le.LinkExtractor(
        restrict_css="#overview div.container div.row"
    )

    def parse(self, response):
        for category in self.lx_categories.extract_links(response):
            yield scrapy.Request(category.url, self.parse_category_root)

    def parse_category_root(self, response):
        cat_title = response.css("#contents h1.Title::text").get()

