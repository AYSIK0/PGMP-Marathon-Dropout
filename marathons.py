from abc import ABC, abstractmethod
from typing import Final
from itertools import chain
from bs4 import BeautifulSoup
import requests


class MarathonBase(ABC):
    """
    ### Abstract base class that implement some shared functionality used by children classes.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
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
    def request_page(self) -> requests.models.Response:
        """
        ### Abstract method, specific implementation handled by child class. \n
        the method should be used to request a webpage.
        """
        raise NotImplementedError(
            "Abstract class method was called, this method should be overridden in child class."
        )

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
    def get_max_pages(self) -> list[str]:
        """
        ### Abstract method, specific implementation handled by child class; \n
        the method is used to get the max number of pages available based on \
        the number of results displayed.
        """
        raise NotImplementedError(
            "Abstract class method was called, this method should be overridden in child class."
        )


class LondonMarathon(MarathonBase):
    """
    ### Class used to gather data of the London marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, url: str, year: str, idps: list[str]) -> list[str]:
        return super().prepare_split_urls(url, year, idps)

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

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(
        self, year: str, num_results: str = "25", after_18: bool = False
    ) -> list[str]:
        """
        ### Method used for getting the max page number for both men and women result pages.
        ---
        ### Arguments:
        - year: The year of marathon
        - num_results: The number of results to be displayed per page (Default 25).
        - after_18 (Bool): Wether we getting page after 2018.
        ---
        ### Returns: A list with 2 elements, the first and second is max page number for men and women respectively.
        """
        web_pages = self.request_page(year, pages=["1", "1"], num_results=num_results)
        men_soup = self.create_soup(webpage_content=web_pages[0].content)
        women_soup = self.create_soup(webpage_content=web_pages[1].content)

        if after_18:
            # The fifth Anchor tag starting from last is the total number of pages.
            return (men_soup.findAll("a")[-5].text, women_soup.findAll("a")[-5].text)
        else:
            # Anchor tag before last is the total number of pages.
            return (men_soup.findAll("a")[-2].text, women_soup.findAll("a")[-2].text)


class HamburgMarathon(MarathonBase):
    """
    Class used to gather data of the Hamburg marathon which will be used for scraping it.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)

    def prepare_res_urls(
        self,
        year: str,
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        return super().prepare_res_urls(
            self.url_template, year, pages, gender, num_results, flat_list
        )

    def prepare_split_urls(self, url: str, year: str, idps: list[str]) -> list[str]:
        return super().prepare_split_urls(url, year, idps)

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

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        return super().create_soup(webpage_content)

    def get_max_pages(
        self,
        year: str,
        num_results: str = "25",
    ) -> list[str]:
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

        # The fifth Anchor tag starting from last is the total number of pages.
        return (men_soup.findAll("a")[-5].text, women_soup.findAll("a")[-5].text)
