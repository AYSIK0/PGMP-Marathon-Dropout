import os


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
