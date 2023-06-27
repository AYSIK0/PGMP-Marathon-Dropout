import scrapy
from ..items import StockholmItem, StockholmSplitItem
import logging
import re


class Stockholm2122(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Stockholm marathon data between 2021 - 2022.
    """

    name = "stockholm21_22"

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

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        # Scraping Splits.
        split_item = StockholmSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = StockholmSplitItem.get_split_keys()
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

        if (
            # Check if runners have finish split (including empty one).
            split_item.get("k_finish")
            # Check if time is available (some runners might have finish speed or pace but no time).
            and split_item["k_finish"][0]
            and re.match("(\d{2}:\d{2}:\d{2})", split_item["k_finish"][0])
        ):
            split_item["race_state"] = "Finished"

        # Scraping YOB: year of birth.
        yob_row = response.xpath('//div[@class="detail-box box-general"]//tr')[5]
        split_item["yob"] = yob_row.xpath("td[1]/text()").get()

        yield split_item
