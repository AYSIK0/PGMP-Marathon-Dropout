import os
import scrapy
from scrapy.crawler import CrawlerRunner
from crochet import setup, wait_for
import pandas as pd

# crochet setup.
setup()


def get_settings(
    file_name: str = "Default",
    fields: list[str] = None,
    _format: str = "csv",
    overwrite: bool = True,
    data_path: str = "Marathons_Data/",
) -> dict:
    """
    ### Function to generate settings for scrapy spider.
    ---
    ### Arguments:
    + file_name: File name where the scraped data will be saved.
    + fields: The name of fields (columns).
    + _format: The file format (Default: CSV).
    + overwrite: Wether the spider should overwrite the file if it already exists (Default: True)
    + data_path: The path where the file will be saved
    ---
    ### Returns:
    A dictionary that contains the settings used by a spider.
    """
    working_directory = os.getcwd()
    settings = {
        "FEEDS": {
            f"{working_directory}/{data_path}/{file_name}.{_format}": {
                "format": f"{_format}",
                "fields": fields,
                "overwrite": str(overwrite),
            }
        },
        "LOG_LEVEL": "INFO",
    }
    return settings


@wait_for(10)
def run_spider(
    spider: scrapy.Spider,
    urls: list[str],
    settings: dict,
    splits: bool = False,
    **kwargs,
) -> None:
    """
    ### Function to run spiders using crochet.
    ---
    ### Arguments:
    - spider: Scrapy spider to run.
    - urls: A list of URLs to be scraped by the spider.
    - settings: Spider settings, will overwrite its default settings.
    - splits: A boolean to let the spider wether it is scraping the split page.
    - **kwargs: for other keyword-only arguments.
    ---
    ### Returns:
    None
    """
    crawler_run = CrawlerRunner(settings)
    crawler_run.crawl(spider, urls=urls, splits=splits, **kwargs)


def expand_splits(
    dataframe: pd.DataFrame,
    keys: list[str] = [
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
    ],
):
    """
    ### Function to expand a split column (the ones in keys arg) into 3 new columns since each split column contains a list with 3 elements [time, pace, speed]. \
    the new column will have the name of the split then one of the following suffix (_time, _pace, _speed); also it will replace `None` with `'-'`
    ### N.B if one of the `keys` is not a column in the original Dataframe, the returned Dataframe will still contains 3 columns for the missing column. 
    ---
    ### Arguments:
    - dataframe: The Dataframe with the original data.
    - keys: Names of column to expand.
    ---
    ### Returns:
    A new Dataframe with new columns
    """
    translate_table = {"[": "", "N": "", "o": "-", "n": "", "e": "", "]": ""}
    for key in keys:
        if key in dataframe.columns:
            dataframe[key] = dataframe[key].str.translate(
                str.maketrans(translate_table)
            )
            dataframe[[f"{key}_time", f"{key}_pace", f"{key}_speed"]] = dataframe[
                key
            ].str.split(",", expand=True)
        else:
            dataframe = dataframe.reindex(
                dataframe.columns.to_list()
                + [key, f"{key}_time", f"{key}_pace", f"{key}_speed"],
                axis=1,
            )
    return dataframe
