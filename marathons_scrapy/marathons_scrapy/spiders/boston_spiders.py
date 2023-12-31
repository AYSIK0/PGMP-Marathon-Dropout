import scrapy
from ..items import BostonItem, BostonSplitItem
import logging
import re


class Boston1417(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Boston marathon data between 2014 - 2017
    """

    name = "boston14_17"

    def __init__(self, urls: list[str], splits: bool = False, **kwargs):
        self.urls: str = urls
        self.splits: bool = splits
        self.first_split_idx = (
            kwargs.get("first_split_idx") if kwargs.get("first_split_idx") else 1
        )
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
        item = BostonItem()
        for runner in runners[1:]:  # skipping table header.
            item["run_no"] = runner.xpath("td[5]/text()").get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath("td[7]/text()").get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath("td[4]/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = BostonSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        split_item["race_state"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[1]/td/text()'
        ).get()
        split_item["last_split"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[2]/td/text()'
        ).get()

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = BostonSplitItem.get_split_keys()

        for i, split in enumerate(
            splits[self.first_split_idx :]
        ):  # 10 rows in each splits table.
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath("td[2]/text()").get()  # time hh:mm:ss
                pace = split.xpath("td[4]/text()").get()  # min/mile
                speed = split.xpath("td[5]/text()").get()  # miles/h
            else:
                time = "-"
                pace = "-"
                speed = "-"
            split_item[keys[i]] = [time, pace, speed]

        # Getting runner age group.
        age_group = response.xpath(
            '//div[@class="detail-box box-general"]//tr[3]/td/text()'
        ).get()
        if age_group:
            # extracting age category from the age group. (e.g Female 18-39 -> 18-39) (e.g Female 80+ -> 80+)
            age_cat = re.search("(\d{2}-\d{2})|(\d{2}.+?)", age_group)
            if age_cat:
                split_item["age_cat"] = age_cat.group()
            else:
                split_item["age_cat"] = age_group

        yield split_item


class Boston1823(scrapy.Spider):
    """
    ### Scrapy spider used to scrap Boston marathon data between 2018 - 2023
    """

    name = "boston18_23"

    def __init__(self, urls: list[str], splits: bool = False, **kwargs):
        self.urls: str = urls
        self.splits: bool = splits
        self.first_split_idx = (
            kwargs.get("first_split_idx") if kwargs.get("first_split_idx") else 1
        )
        self.splits_names = {
            "5K",
            "10K",
            "15K",
            "20K",
            "HALF",
            "25K",
            "30K",
            "35K",
            "40K",
            "Finish Net",
        }
        self.run_div_idx = 3
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
            # This is needed since the run_no between 2018 - 2019 is div[3]; from 2021 onward it is div[2]
            year = int(re.search("(?<=/)\d{4}(?=/)", urls[0]).group())
            if year > 2019:
                self.run_div_idx = 2
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        ### Parse the main result pages.
        """
        runners = response.xpath('//li[contains(@class, " list-group-item row")]')
        item = BostonItem()
        for runner in runners:
            item["run_no"] = runner.xpath(
                f'.//div[@class="pull-left"]/div/div[{self.run_div_idx}]/text()'
            ).get()
            item["gender"] = re.findall(".(?=&num)", response.url)[0]
            item["finish"] = runner.xpath(
                './/div[@class="pull-right"]/div/div[2]/text()'
            ).get()
            item["idp"] = re.findall(
                "(?<=idp=).+?(?=&)", runner.xpath(".//h4/a/@href").get()
            )[0]
            yield item

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = BostonSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]
        split_item["race_state"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[1]/td/text()'
        ).get()
        split_item["last_split"] = response.xpath(
            '//div[@class="detail-box box-state"]//tr[2]/td/text()'
        ).get()

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = BostonSplitItem.get_split_keys()

        # This is used to keep count of how many splits need be skipped since boston marathon provide extra splits in miles.
        extra_splits = 0
        for i, split in enumerate(splits[self.first_split_idx :]):
            if split.xpath("th/text()").get().strip() in self.splits_names:
                if "estimated" not in split.xpath("@class").get():
                    time = split.xpath("td[2]/text()").get()  # time hh:mm:ss
                    pace = split.xpath("td[4]/text()").get()  # min/mile
                    speed = split.xpath("td[5]/text()").get()  # miles/h
                else:
                    time = "-"
                    pace = "-"
                    speed = "-"
            else:
                extra_splits += 1
            split_item[keys[i - extra_splits]] = [time, pace, speed]

        # Getting runner age group.
        age_group = response.xpath(
            '//div[@class="detail-box box-general"]//tr[3]/td/text()'
        ).get()
        if age_group:
            # extracting age category from the age group. (e.g Female 18-39 -> 18-39) (e.g Female 80+ -> 80+)
            age_cat = re.search("(\d{2}-\d{2})|(\d{2}.+?)", age_group)
            if age_cat:
                split_item["age_cat"] = age_cat.group()
            else:
                split_item["age_cat"] = age_group

        yield split_item
