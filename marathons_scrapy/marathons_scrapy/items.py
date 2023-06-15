# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MarathonsScrapyItem(scrapy.Item):
    # define the fields for your item here like:
    run_no: str = scrapy.Field()
    age_cat: str = scrapy.Field()
    gender: str = scrapy.Field()
    half: str = scrapy.Field()
    finish: str = scrapy.Field()
    idp: str = scrapy.Field()


class MarathonsSplitItem(scrapy.Item):
    """ """

    race_state: str = scrapy.Field()
    last_split: str = scrapy.Field()
    idp: str = scrapy.Field()
    k_5: list = scrapy.Field()
    k_10: list = scrapy.Field()
    k_15: list = scrapy.Field()
    k_20: list = scrapy.Field()
    k_half: list = scrapy.Field()
    k_25: list = scrapy.Field()
    k_30: list = scrapy.Field()
    k_35: list = scrapy.Field()
    k_40: list = scrapy.Field()
    k_finish: list = scrapy.Field()

    @classmethod
    def get_split_keys(self) -> list[str]:
        return [
            "k_5",
            "k_10",
            "k_15",
            "k_20",
            "k_half",
            "k_25",
            "k_30",
            "k_35",
            "k_40",
            "k_finish",
        ]


class LondonItem(MarathonsScrapyItem):
    def __init__(self):
        super().__init__()


class LondonSplitItem(MarathonsSplitItem):
    def __init__(self):
        super().__init__()


class HamburgItem(MarathonsScrapyItem):
    def __init__(self):
        super().__init__()


class HamburgSplitItem(MarathonsSplitItem):
    def __init__(self):
        super().__init__()


class HoustonItem(MarathonsScrapyItem):
    def __init__(self):
        super().__init__()


class HoustonSplitItem(MarathonsSplitItem):
    def __init__(self):
        super().__init__()
