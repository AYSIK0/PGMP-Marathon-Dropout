import scrapy
from ..items import StockholmItem, StockholmSplitItem
import logging
import re


class Stockholm2122(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Stockholm marathon data between 2021 - 2022.
    """

    name = "stockholm21_22"

    def __init__(self, urls: list[str], splits: bool = False):
        self.urls: str = urls
        self.splits: bool = splits
        super().__init__()

    # Logging info
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=logging.INFO,
    )

    def start_requests(self):
        urls = self.urls
        if self.splits:
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse_split)
        else:
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        ### Parse the main result pages.
        """
        runners = response.xpath('//li[contains(@class, " list-group-item row")]')
        item = StockholmItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                './/div[@class= " list-field type-field"]/text()'
            ).get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath(
                './/div[@class="right list-field type-time"]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item
