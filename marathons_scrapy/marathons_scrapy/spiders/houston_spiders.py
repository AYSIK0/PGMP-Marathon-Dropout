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

    def parse_split(self, response):
        """
        ### Parse the split result pages.
        """
        split_item = HoustonSplitItem()
        split_item["idp"] = re.findall("(?<=idp=).+?(?=&)", response.url)[0]

        splits = response.xpath('//div[@class="detail-box box-splits"]//tr')
        keys = HoustonSplitItem.get_split_keys()
        for i, split in enumerate(splits[1:]):  # 10 rows in each splits table.
            # check if the time is not estimated.
            if "estimated" not in split.xpath("@class").get():
                time = split.xpath("td[2]/text()").get()  # time hh:mm:ss
                pace = split.xpath("td[4]/text()").get()  # min/mile
                speed = split.xpath("td[5]/text()").get()  # miles/h
            else:
                time = "-"
                pace = "-"
                speed = "-"
            split_item[keys[i]] = [time, pace, speed]

        # Totals table in runner split page.
        total = response.xpath('//div[@class="detail-box box-totals"]//tr')[3]

        # This not actual finish_status since "Finish Net" is the field being scraped (a row in Totals table),
        # this row includes the finish time for runners that did finish, for other runners it displayed DNF (Did Not FInish) or DSQ (Disqualified).
        finish_status = total.xpath("td[1]/text()").get()

        if re.match("(\d{2}:\d{2}:\d{2})", finish_status):
            split_item["race_state"] = "Finished"
        else:
            match finish_status.lower():  # Python 3.10.x+
                case "dnf":
                    split_item["race_state"] = "DNF"
                case "dq - over 6h" | "dq - over 6hs" | "dq over 6 hours" | "dq - over 6 hrs" | "over 6h":
                    split_item["race_state"] = "DQ - Over 6h"
                case "dq - switch from half to mara":
                    split_item["race_state"] = "DQ - SWITCH from HALF to MARA"
                case "dq - missing split" | "missing splits":
                    split_item["race_state"] = "DQ - missing split"
                case "dq" | "dq -":
                    split_item["race_state"] = "DQ - No Reason Was Given"
                case "dns":
                    split_item["race_state"] = "DNS -  Did Not Start"
                case _:
                    split_item["race_state"] = "Other"

        yield split_item
