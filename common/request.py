import requests
from bs4 import BeautifulSoup
from common.errors import HttpFetchError


#TODO support proxy
class Request:
    def get(self, url):
        return requests.get(url)


def get_soup(url):
    try:
        # print("requesting {}".format(url))
        request = Request()
        resp = request.get(url)
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        raise HttpFetchError() from e