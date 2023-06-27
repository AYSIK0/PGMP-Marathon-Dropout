from abc import ABC, abstractmethod
from typing import Final
from itertools import chain
import requests
from bs4 import BeautifulSoup
from utils.scrap_utils import get_settings


class MarathonBase(ABC):
    """
    ### Abstract base class that implement some shared functionality used by children classes.
    """

    def __init__(self, url_template: str = None, split_url_template: str = None):
        """
        ### Construct the marathon object.
        ---
        ### Arguments:
        - url_template: The base URL template of the marathon result page.
        - split_url_template: The base URL of the runners splits page.
        """
        if not url_template or not split_url_template:
            raise ValueError("url_template or split_url_template is None")
        else:
            self._BASE_URL: Final[str] = url_template
            self._SPLIT_URL: Final[str] = split_url_template

    @property
    def url_template(self):
        return self._BASE_URL

    @url_template.setter
    def url_template(self, val):
        raise Exception(
            "Attempting to change Result URL of the class, if you want to change it reinitialize the object with the new URL (url_template='new_url')."
        )

    @property
    def split_url_template(self):
        return self._SPLIT_URL

    @split_url_template.setter
    def split_url_template(self, val):
        raise Exception(
            "Attempting to change Split URL of the class, if you want to change it reinitialize the object with the new URL (split_url_template='new_url')."
        )

    @abstractmethod
    def prepare_res_urls(
        self,
        url: str,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]] | list[str]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - url: URL template to use.
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        if len(pages) != 2:
            raise Exception(
                " pages should contains exactly 2 elements first element is men max page number and the second one is the women max page number."
            )
        men_pages = int(pages[0])
        women_pages = int(pages[1])
        max_pages = max(men_pages, women_pages)
        res_urls = [[], []]

        # for year in years:
        for page in range(1, max_pages + 1):
            # Men
            if page <= men_pages:
                res_urls[0].append(url.format(year, str(page), gender[0], num_results))
            # Women
            if page <= women_pages:
                res_urls[1].append(url.format(year, str(page), gender[1], num_results))
        if flat_list:
            # unpacking the lists into a single list.
            return list(chain(*res_urls))
        return res_urls

    @abstractmethod
    def prepare_split_urls(self, url: str, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - url: URL template to use.
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        res_list = []
        for idp in idps:
            res_list.append(url.format(year, idp))
        return res_list

    @abstractmethod
    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        curr_url = self.prepare_res_urls(
            year=year, pages=pages, num_results=num_results
        )
        try:
            men_res_page = requests.get(curr_url[0][0])
            women_res_page = requests.get(curr_url[1][0])
            return (men_res_page, women_res_page)
        except Exception as e:
            print(f"Error Occurred: {e}")

    @abstractmethod
    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        """
        ### Create a BeautifulSoup object based on the content of a webpage.
        ---
        ### Arguments:
        - webpage_content: Webpage content such as the one returned by requests module.
        ---
        ### Returns:
        BeautifulSoup object.
        """
        return BeautifulSoup(webpage_content, features="lxml")

    @abstractmethod
    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        web_pages = self.request_page(year, pages=["1", "1"], num_results=num_results)
        men_soup = self.create_soup(webpage_content=web_pages[0].content)
        women_soup = self.create_soup(webpage_content=web_pages[1].content)

        max_men_pages = men_soup.select('div[class*="pages"] a')[-2].text
        max_women_pages = women_soup.select('div[class*="pages"] a')[-2].text
        return (max_men_pages, max_women_pages)

    @abstractmethod
    def gen_res_scrap_info(
        self,
        name: str,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - name: Name of marathon
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        _max_pages = self.get_max_pages(year, num_results)
        print(f"Men Pages: {_max_pages[0]} || Women Pages: {_max_pages[1]}")

        pages_urls = self.prepare_res_urls(
            year,
            [_max_pages[0], _max_pages[1]],
            num_results=num_results,
            flat_list=True,
        )
        print(f"{name} {year} total results pages: {len(pages_urls)}")
        print(f"Example URLs: \n {pages_urls[0]} \n {pages_urls[int(_max_pages[0])]}")

        res_settings = get_settings(
            file_name=f"{name}{year}_res",
            fields=scraped_fields,
            _format="csv",
            overwrite=True,
            data_path=data_path,
        )

        if show_settings:
            print(f"Settings: \n{res_settings}")

        return (pages_urls, res_settings)

    @abstractmethod
    def gen_splits_scrap_info(
        self,
        name: str,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - name: Name of marathon.
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        splits_urls = self.prepare_split_urls(year, idps)

        split_settings = get_settings(
            file_name=f"{name}{year}_splits",
            fields=scraped_fields,
            _format="csv",
            overwrite=True,
            data_path=data_path,
        )

        print(f"{name} {year} total splits pages: {len(splits_urls)}")
        print(f"Example URLs: \n {splits_urls[0]} \n {splits_urls[-1]}")
        if show_settings:
            print(split_settings)

        return (splits_urls, split_settings)


class LondonMarathon(MarathonBase):
    """
    ### Class used to gather data of the London marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)
        self.__NAME = "London"

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        return super().request_page(year, pages, num_results)

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_res_scrap_info(
            self.__NAME, year, num_results, scraped_fields, data_path, show_settings
        )

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_splits_scrap_info(
            self.__NAME, year, idps, scraped_fields, data_path, show_settings
        )


class HamburgMarathon(MarathonBase):
    """
    Class used to gather data of the Hamburg marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)
        self.__NAME = "Hamburg"

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        return super().request_page(year, pages, num_results)

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_res_scrap_info(
            self.__NAME, year, num_results, scraped_fields, data_path, show_settings
        )

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_splits_scrap_info(
            self.__NAME, year, idps, scraped_fields, data_path, show_settings
        )


class HoustonMarathon(MarathonBase):
    """
    ### Class used to gather data of the Houston marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)
        self.__NAME = "Houston"

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        return super().request_page(year, pages, num_results)

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_res_scrap_info(
            self.__NAME, year, num_results, scraped_fields, data_path, show_settings
        )

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_splits_scrap_info(
            self.__NAME, year, idps, scraped_fields, data_path, show_settings
        )


class StockHolmMarathon(MarathonBase):
    """
    ### Class used to gather data of the Stockholm marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)
        self.__NAME = "Stockholm"

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        return super().request_page(year, pages, num_results)

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_res_scrap_info(
            self.__NAME, year, num_results, scraped_fields, data_path, show_settings
        )

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_splits_scrap_info(
            self.__NAME, year, idps, scraped_fields, data_path, show_settings
        )


class BostonMarathon(MarathonBase):
    """
    ### Class used to gather data of the Boston marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)
        self.__NAME = "Boston"

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of the marathon.
        - idps: idp of the runner.
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        return super().request_page(year, pages, num_results)

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_res_scrap_info(
            self.__NAME, year, num_results, scraped_fields, data_path, show_settings
        )

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        return super().gen_splits_scrap_info(
            self.__NAME, year, idps, scraped_fields, data_path, show_settings
        )


class ChicagoMarathon(MarathonBase):
    """
    ### Class used to gather data of the Chicago marathon which will be used for scraping it.
    """

    def __init__(
        self,
        url_template: str = None,
        split_url_template: str = None,
        event_id: str = None,
    ):
        super().__init__(url_template, split_url_template)
        self.__NAME = "Chicago"
        self.event_id = event_id

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        event_id: str,
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]] | list[str]:
        """
        ### Method that creates the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - year: year of marathon as string.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - event_id: Event id to only select 'marathon' runners.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        - flat_list: A boolean to decided wether to return a single list that contains \
            all URLs or a list with 2 (men and women) inner URLs list
        ---
        ### Returns:
        #### flat_list == False:
        A list that contain two lists, the first one has all URLs of the men and the second one contains URLs of the women.
        #### flat_list == True:
        A list that contains all URLs for men and women pages.
        """
        if len(pages) != 2:
            raise Exception(
                " pages should contains exactly 2 elements first element is men max page number and the second one is the women max page number."
            )
        men_pages = int(pages[0])
        women_pages = int(pages[1])
        max_pages = max(men_pages, women_pages)
        res_urls = [[], []]

        # for year in years:
        for page in range(1, max_pages + 1):
            # Men
            if page <= men_pages:
                res_urls[0].append(
                    self.url_template.format(
                        year, str(page), gender[0], num_results, event_id
                    )
                )
            # Women
            if page <= women_pages:
                res_urls[1].append(
                    self.url_template.format(
                        year, str(page), gender[1], num_results, event_id
                    )
                )
        if flat_list:
            # unpacking the lists into a single list.
            return list(chain(*res_urls))
        return res_urls

    def prepare_split_urls(self, year: str, idps: list[str]) -> list[str]:
        """
        ### Method that create the personal splits URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        -
        ---
        ### Returns:
        A list that contains all URLs for split page of the runner.
        """
        return super().prepare_split_urls(self.split_url_template, year, idps)

    def request_page(
        self, year: str = None, pages: list[str] = None, num_results: str = "25"
    ) -> tuple[requests.models.Response, requests.models.Response]:
        """
        ### Method to request an HTML page.
        ---
        ### Arguments:
        - year: The year of the marathon.
        - page: A list that must only contain two elements the max pages for Men and Women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A tuple with two elements, the first contain the men webpage and the second contains the women webpage.
        """
        curr_url = self.prepare_res_urls(
            year=year, pages=pages, event_id=self.event_id, num_results=num_results
        )
        try:
            men_res_page = requests.get(curr_url[0][0])
            women_res_page = requests.get(curr_url[1][0])
            return (men_res_page, women_res_page)
        except Exception as e:
            print(f"Error Occurred: {e}")

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(self, year: str, num_results: str = "25") -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        return super().get_max_pages(year, num_results)

    def gen_res_scrap_info(
        self,
        year: str,
        num_results: str,
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Method to generate the URLs for the results pages of a marathon, \
        the number of URLs dependents on the max pages found in a page with the specified number of results.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default: 25).
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - show_settings: Bool to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        _max_pages = self.get_max_pages(year, num_results)
        print(f"Men Pages: {_max_pages[0]} || Women Pages: {_max_pages[1]}")
        # Results Pages URLs
        pages_urls = self.prepare_res_urls(
            year,
            [_max_pages[0], _max_pages[1]],
            event_id=self.event_id,
            num_results=num_results,
            flat_list=True,
        )
        print(f"{self.__NAME} {year} total results pages: {len(pages_urls)}")
        print(f"Example URLs: \n {pages_urls[0]} \n {pages_urls[int(_max_pages[0])]}")

        # Spider settings.
        res_settings = get_settings(
            file_name=f"{self.__NAME}{year}_res",
            fields=scraped_fields,
            _format="csv",
            overwrite=True,
            data_path=data_path,
        )

        if show_settings:
            print(f"Settings: \n{res_settings}")

        return (pages_urls, res_settings)

    def gen_splits_scrap_info(
        self,
        year: str,
        idps: list[str],
        scraped_fields: list[str],
        data_path: str,
        show_settings: bool = False,
        use_event_id: bool = False,
    ) -> tuple[list[str], dict]:
        """
        ### Function to generate the URLs for runners splits pages, based on idps.
        ---
        ### Arguments:
        - year: The year of marathon.
        - idps: List of runners ids.
        - scraped_fields: The fields which will be saved from the scraped data.
        - data_path: The path to save the scraped data file.
        - use_event_id: Bool, to use_event_id instead of year.
        - show_settings: Bool, to print the settings created (Default: False).
        ---
        ### Returns: A tuple with 2 elements, the first is a list of URLs while the second is dictionary of settings.
        """
        if use_event_id:
            splits_urls = self.prepare_split_urls(self.event_id, idps)
        else:
            splits_urls = self.prepare_split_urls(year, idps)

        split_settings = get_settings(
            file_name=f"{self.__NAME}{year}_splits",
            fields=scraped_fields,
            _format="csv",
            overwrite=True,
            data_path=data_path,
        )

        print(f"{self.__NAME} {year} total splits pages: {len(splits_urls)}")
        print(f"Example URLs: \n {splits_urls[0]} \n {splits_urls[-1]}")
        if show_settings:
            print(f"Settings: \n{split_settings}")

        return (splits_urls, split_settings)
