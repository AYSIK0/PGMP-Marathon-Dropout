import scrapy
from ..items import HamburgItem, HamburgSplitItem
import logging
import re


class Hamburg1317(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Hamburg marathon data between 2013 - 2017
    """

    name = "hamburg13_17"

    def __init__(self, urls: list[str], splits: bool = False, **kwargs):
        self.urls: str = urls
        self.splits: bool = splits
        super().__init__()

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
        runners = response.xpath("//tr")
        item = HamburgItem()
        for runner in runners[1:]:  # skipping table header.
            item["run_no"] = runner.xpath("td[3]/text()").get()
            item["age_cat"] = runner.xpath("td[6]/text()").get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath("td[8]/text()").get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath("td[4]/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = HamburgSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = HamburgSplitItem.get_split_keys()
        # Extracting splits data.
        for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
            # check if the time is not estimated.
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath("td[1]/text()").get()  # time hh:mm:ss
                pace = split.xpath("td[3]/text()").get()  # min/km
                speed = split.xpath("td[4]/text()").get()  # km/h
            else:
                time = "-"
                pace = "-"
                speed = "-"
            split_item[keys[i]] = [time, pace, speed]
        yield split_item


class Hamburg1823(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Hamburg marathon data between 2018 - 2023
    """

    name = "hamburg18_23"

    def __init__(self, urls: list[str], splits: bool = False, **kwargs):
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
        item = HamburgItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                './/div[@class= " list-field type-field"]/text()'
            ).get()
            item["age_cat"] = runner.xpath(
                './/div[@class= " list-field type-age_class"]/text()'
            ).get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath(
                './/div[@class=" list-field type-time"]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = HamburgSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = HamburgSplitItem.get_split_keys()
        for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
            # check if the time is not estimated.
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath("td[2]/text()").get()  # time hh:mm:ss
                pace = split.xpath("td[4]/text()").get()  # min/km
                speed = split.xpath("td[5]/text()").get()  # km/h
            else:
                time = "-"
                pace = "-"
                speed = "-"
            split_item[keys[i]] = [time, pace, speed]
        yield split_item
