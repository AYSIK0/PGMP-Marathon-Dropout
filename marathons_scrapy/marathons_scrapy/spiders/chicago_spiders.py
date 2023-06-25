import scrapy
from ..items import ChicagoItem, ChicagoSplitItem
import logging
import re


class Chicago1422(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Chicago marathon data between 2014 - 2022
    """

    name = "chicago14_23"

    def __init__(
        self, urls: list[str], splits: bool = False, first_split_idx: int = 1, **kwargs
    ):
        self.urls: str = urls
        self.splits: bool = splits
        self.first_split_idx = first_split_idx
        self.year = kwargs.get("year")
        super().__init__()

    # Logging info
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=logging.INFO,
    )

    def start_requests(self):
        urls = self.urls
        if self.splits:
            # TODO Test it
            # callback = self.parse_split_22 if self.year == "2022" else self.parse_split
            # for url in urls:
            #     yield scrapy.Request(url=url, callback=callback)
            pass
        else:
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        ### Parse the main result pages.
        """
        runners = response.xpath('//li[contains(@class, " list-group-item row")]')
        item = ChicagoItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                './/div[@class="pull-left"]/div/div[1]/text()'
            ).get()
            item["age_cat"] = runner.xpath(
                './/div[@class="pull-left"]/div/div[2]/text()'
            ).get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath(
                './/div[@class="pull-right"]/div/div[1]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item
