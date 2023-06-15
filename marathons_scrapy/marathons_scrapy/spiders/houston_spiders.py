import scrapy
from ..items import HoustonItem, HoustonSplitItem
import logging
import re


class Houston1819(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Houston marathon data between 2018 - 2019
    """

    name = "houston18_19"

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
        item = HoustonItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                './/div[@class= " list-field type-field"]/text()'
            ).get()
            item["age_cat"] = runner.xpath(
                './/div[@class= " list-field type-age_class"]/text()'
            ).get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath(
                './/div[@class="split list-field type-time"]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item

    # def parse_split(self, response):
    #     """
    #     ### Parse the split result pages.
    #     """
    #     split_item = HoustonSplitItem()
    #     split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]

    #     splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
    #     keys = HoustonSplitItem.get_split_keys()
    #     for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
    #         # check if the time is not estimated.
    #         if "estimated" not in split.xpath("@class").get():
    #             time = split.xpath("td[2]/text()").get()  # time hh:mm:ss
    #             pace = split.xpath("td[4]/text()").get()  # min/km
    #             speed = split.xpath("td[5]/text()").get()  # km/h
    #         else:
    #             time = "-"
    #             pace = "-"
    #             speed = "-"
    #         split_item[keys[i]] = [time, pace, speed]
    #     yield split_item
