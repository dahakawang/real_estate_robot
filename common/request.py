import requests
from bs4 import BeautifulSoup
from common.errors import *


#TODO support proxy
class Request:
    def get(self, url):
        return requests.get(url)


def get_soup(url):
    try:
        # print("requesting {}".format(url))
        request = Request()
        resp = request.get(url)

        if resp.status_code == 404:
            raise KnownError("not found error (404)".format(url))
        if resp.status_code != 200:
            raise HttpFetchError("remote server returned {}".format(resp.status_code))

        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        raise HttpFetchError() from e


if __name__ == "__main__":
    get_soup("https://sh.lianjia.com/ershoufang/107002002775.html")