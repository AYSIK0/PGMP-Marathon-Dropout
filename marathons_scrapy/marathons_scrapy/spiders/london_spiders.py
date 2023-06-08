import scrapy
from ..items import LondonItem, LondonSplitItem
import logging
import re


# TODO DELETE
# Command to test: scrapy crawl london14_18 -a urls=None -O london14_test.csv
# Command to test splits: scrapy crawl london14_18 -a splits=True -a urls=None -O london14_splits_test.csv


class LondonSpider1418(scrapy.Spider):
    """
    ### Scrapy spider used to scrap London marathon data between 2014 - 2018
    """

    name = "london14_18"

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
        runners = response.xpath("//tr")
        item = LondonItem()
        for runner in runners[1:]:  # skipping table header.
            item["run_no"] = runner.xpath("td[6]/text()").get()
            item["age_cat"] = runner.xpath("td[7]/text()").get()
            item["sex"] = re.findall(".(?=&num)", response.url)[0]
            item["half"] = runner.xpath("td[8]/text()").get()
            item["finish"] = runner.xpath("td[9]/text()").get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath("td[4]/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = LondonSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        split_item["race_state"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[1]/td/text()'
        ).get()
        split_item["last_split"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[2]/td/text()'
        ).get()

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = LondonSplitItem.get_split_keys()
        for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
            time = split.xpath("td[2]/text()").get()  # time
            pace = split.xpath("td[4]/text()").get()  # min/km
            speed = split.xpath("td[5]/text()").get()  # km/h
            split_item[keys[i]] = [time, pace, speed]
        yield split_item


# Command to test: scrapy crawl london19_23 -a urls=None -O london19_test.csv
# Command to test splits: scrapy crawl london19_23 -a splits=True -a urls=None -O london19_splits_test.csv
class LondonSpider1923(scrapy.Spider):
    """
    ### Scrapy spider used to scrap London marathon data between 2019 - 2023
    """

    name = "london19_23"

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
        item = LondonItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                './/div[@class= " list-field type-field"]/text()'
            ).get()
            item["age_cat"] = runner.xpath(
                './/div[@class= " list-field type-age_class"]/text()'
            ).get()
            item["sex"] = re.findall(".(?=&num)", response.url)[0]
            item["half"] = runner.xpath(
                './/div[@class="split list-field type-time hidden-xs"]/text()'
            ).get()
            item["finish"] = runner.xpath(
                './/div[@class="split list-field type-time"]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = LondonSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        split_item["race_state"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[1]/td/text()'
        ).get()
        split_item["last_split"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[2]/td/text()'
        ).get()

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = LondonSplitItem.get_split_keys()
        for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
            time = split.xpath("td[2]/text()").get()  # time
            pace = split.xpath("td[4]/text()").get()  # min/km
            speed = split.xpath("td[5]/text()").get()  # km/h
            split_item[keys[i]] = [time, pace, speed]
        yield split_item
