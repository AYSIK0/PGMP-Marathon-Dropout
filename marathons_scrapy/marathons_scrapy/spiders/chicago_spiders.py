import scrapy
from ..items import ChicagoItem, ChicagoSplitItem
import logging
import re


class Chicago1422(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Chicago marathon data between 2014 - 2022
    """

    name = "chicago14_23"

    def __init__(self, urls: list[str], splits: bool = False, **kwargs):
        self.urls: str = urls
        self.splits: bool = splits
        self.first_split_idx = (
            kwargs.get("first_split_idx") if kwargs.get("first_split_idx") else 1
        )
        self.year = kwargs.get("year")
        self.finish_div_idx = 1
        super().__init__()

    # Logging info
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=logging.INFO,
    )

    def start_requests(self):
        urls = self.urls
        if self.splits:
            # This solution only works for 2022, find better solution for future marathon.
            callback = self.parse_split_22 if self.year == "2022" else self.parse_split
            for url in urls:
                yield scrapy.Request(url=url, callback=callback)
            pass
        else:
            # This is needed since the finish_time between 2014 - 2021 is //div[1]; for 2022 it is //div[2]
            self.finish_div_idx = 2 if self.year == "2022" else 1
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
                f'.//div[@class="pull-right"]/div/div[{self.finish_div_idx}]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = ChicagoSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = ChicagoSplitItem.get_split_keys()

        for i, split in enumerate(splits[self.first_split_idx :]):
            # check if the time is not estimated.
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath('td[@class="time"]/text()').get()  # time hh:mm:ss
                pace = split.xpath(
                    'td[contains(@class, "min_km")]/text()'
                ).get()  # min/km
                speed = split.xpath('td[contains(@class, "kmh")]/text()').get()  # km/h
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
            split_item["last_split"] = "Finish"

        yield split_item

    def parse_split_22(self, response):
        """
        ### Parse the split result pages for 2022.
        """
        split_item = ChicagoSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        split_item["race_state"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[1]/td/text()'
        ).get()
        split_item["last_split"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[2]/td/text()'
        ).get()

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = ChicagoSplitItem.get_split_keys()

        for i, split in enumerate(splits[self.first_split_idx :]):
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath('td[@class="time"]/text()').get()  # time hh:mm:ss
                pace = split.xpath(
                    'td[contains(@class, "min_km")]/text()'
                ).get()  # min/km
                speed = split.xpath('td[contains(@class, "kmh")]/text()').get()  # km/h
            else:
                time = "-"
                pace = "-"
                speed = "-"
            split_item[keys[i]] = [time, pace, speed]

        yield split_item
