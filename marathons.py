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
        years: list[str],
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        """
        ### Method that create the marathon results URLs needed based on the years and pages lists.
        ---
        ### Arguments:
        - url: URL template to use.
        - years: A list of years as strings.
        - pages: A list that must only contain two elements the max pages for Men and Women.
        - gender: A list of that contains 2 elements M for men and W for women.
        - num_results: The number of results in a page. (Default 25).
        ---
        ### Returns:
        A list that contain two lists, the first one has all URLs of the mens and the second one contains URLs of the women.
        """
        if not years or not pages:
            raise Exception("years and pages should contains at least 1 element.")
        men_pages = int(pages[0])
        women_pages = int(pages[1])
        max_pages = max(men_pages, women_pages)
        res_urls = [[], []]

        for year in years:
            for page in range(1, max_pages + 1):
                # Men
                if page <= men_pages:
                    res_urls[0].append(
                        url.format(year, str(page), gender[0], num_results)
                    )
                # Women
                if page <= women_pages:
                    res_urls[1].append(
                        url.format(year, str(page), gender[1], num_results)
                    )
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
        A list that contains .........!!!!!
        """
        res_list = []
        for idp in idps:
            res_list.append(url.format(year, idp))
        return res_list

    @abstractmethod
    def request_page(self) -> requests.models.Response:
        """
        ### Abstract method, specific implementation handled by child class.
        """
        raise NotImplementedError(
            "Abstract class method was called, this method should be overridden in child class."
        )

    # @abstractmethod
    # def request_pages(self) -> requests.models.Response:
    #     """
    #     ### Abstract method, specific implementation handled by child class.
    #     """
    #     raise NotImplementedError(
    #         "Abstract class method was called, this method should be overridden in child class."
    #     )

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


class LondonMarathon(MarathonBase):
    """
    ### Class used to scrap data of the London marathon.
    """

    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)

    def prepare_res_urls(
        self,
        years: list[str],
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        return super().prepare_res_urls(
            self.url_template, years, pages, gender, num_results, flat_list
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
            years=[year], pages=pages, num_results=num_results
        )
        try:
            men_res_page = requests.get(curr_url[0][0])
            women_res_page = requests.get(curr_url[1][0])
            return (men_res_page, women_res_page)
        except Exception as e:
            print(f"Error Occurred: {e}")

    # def request_pages(
    #     self, years: list[str], pages: list[str], num_results: str = "25"
    # ) -> list[list[requests.models.Response], list[requests.models.Response]]:
    #     """
    #     ### Method to get multiple HTML pages.
    #     ---
    #     ### Arguments:
    #     - years:
    #     - pages:
    #     - num_results:
    #     ---
    #     ### num_results:

    #     """
    #     if not years or not pages:
    #         raise Exception(
    #             "years and pages should contains at least 1 element."
    #         )  # TODO update error message since pages should contains exactly two elements men and women max pages.
    #     temp_urls = self.prepare_res_urls(
    #         years=years, pages=pages, num_results=num_results
    #     )

    #     webpages = [[], []]
    #     print(
    #         f"{len(temp_urls[0] + temp_urls[1])} URLs have been created."
    #     )  # TODO Are these print statements necessary since the file now is a module.!!!
    #     # Men
    #     for url in temp_urls[0]:
    #         try:
    #             res_page = requests.get(url, timeout=10)
    #             webpages[0].append(res_page)
    #         except Exception as e:
    #             print(f"Error Occurred: {e}")
    #     print("Men webpages finished processing.")
    #     # Women
    #     for url in temp_urls[1]:
    #         try:
    #             res_page = requests.get(url, timeout=10)

    #             webpages[1].append(res_page)
    #         except Exception as e:
    #             print(f"Error Occurred: {e}")
    #     print(
    #         "Women webpages finished processing. \n Done"
    #     )  # TODO Are these print statements necessary since the file now is a module.!!!
    #     return webpages

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        """
        ###
        ---
        ### Arguments:
        ---
        ### Returns:
        """
        return super().create_soup(webpage_content)

    def get_max_pages(
        self, year: str, num_results: str = "25", after_18: bool = False
    ) -> list[str]:
        """
        ###
        ---
        ### Arguments:
        -
        -
        - after_18 (Bool): Wether we getting page after 2018.
        ---
        ### Returns:
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
    def __init__(
        self, url_template: str = None, split_url_template: str = None
    ) -> None:
        super().__init__(url_template, split_url_template)

    def prepare_res_urls(
        self,
        years: list[str],
        pages: list[str],
        gender: list[str] = ["M", "W"],
        num_results: str = "25",
        flat_list: bool = False,
    ) -> list[list[str], list[str]]:
        return super().prepare_res_urls(
            self.url_template, years, pages, gender, num_results, flat_list
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
            years=[year], pages=pages, num_results=num_results
        )
        try:
            men_res_page = requests.get(curr_url[0][0])
            women_res_page = requests.get(curr_url[1][0])
            return (men_res_page, women_res_page)
        except Exception as e:
            print(f"Error Occurred: {e}")

    # def request_pages(
    #     self, years: list[str], pages: list[str], num_results: str = "25"
    # ) -> list[list[requests.models.Response], list[requests.models.Response]]:
    #     """
    #     ### Method to get multiple HTML pages.
    #     ---
    #     ### Arguments:
    #     - years:
    #     - pages:
    #     - num_results:
    #     ---
    #     ### num_results:

    #     """
    #     if not years or not pages:
    #         raise Exception(
    #             "years and pages should contains at least 1 element."
    #         )  # TODO update error message since pages should contains exactly two elements men and women max pages.
    #     temp_urls = self.prepare_res_urls(
    #         years=years, pages=pages, num_results=num_results
    #     )

    #     webpages = [[], []]
    #     print(
    #         f"{len(temp_urls[0] + temp_urls[1])} URLs have been created."
    #     )  # TODO Are these print statements necessary since the file now is a module.!!!
    #     # Men
    #     for url in temp_urls[0]:
    #         try:
    #             res_page = requests.get(url, timeout=10)
    #             webpages[0].append(res_page)
    #         except Exception as e:
    #             print(f"Error Occurred: {e}")
    #     print("Men webpages finished processing.")
    #     # Women
    #     for url in temp_urls[1]:
    #         try:
    #             res_page = requests.get(url, timeout=10)

    #             webpages[1].append(res_page)
    #         except Exception as e:
    #             print(f"Error Occurred: {e}")
    #     print(
    #         "Women webpages finished processing. \n Done"
    #     )  # TODO Are these print statements necessary since the file now is a module.!!!
    #     return webpages

    def create_soup(self, webpage_content: bytes) -> BeautifulSoup:
        """
        ###
        ---
        ### Arguments:
        ---
        ### Returns:
        """
        return super().create_soup(webpage_content)

    def get_max_pages(
        self,
        year: str,
        num_results: str = "25",
    ) -> list[str]:
        """
        ###
        ---
        ### Arguments:
        -
        -
        ---
        ### Returns:
        """
        web_pages = self.request_page(year, pages=["1", "1"], num_results=num_results)
        men_soup = self.create_soup(webpage_content=web_pages[0].content)
        women_soup = self.create_soup(webpage_content=web_pages[1].content)

        # The fifth Anchor tag starting from last is the total number of pages.
        return (men_soup.findAll("a")[-5].text, women_soup.findAll("a")[-5].text)