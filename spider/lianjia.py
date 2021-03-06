from urllib.parse import urlparse, urljoin
from common.request import get_soup
from common.threaded_job import post_jobs
from common.mongo_client import *
from common.errors import *
import datetime
import json
import re
import sys
import os
from collections import defaultdict


class ItemSpider:
    def __init__(self, cfg, url, part, area):
        self.url = url
        self.part = part
        self.area = area

    def start_sync(self):
        obj = self._extract()
        self.save_to_mongo(obj)
        return [True]

    def save_to_mongo(self, obj):
        obj["inserted"] = cfg["ts"]
        col = cfg["mongo"]["data"]["raw_lianjia"]
        col.insert_one(obj)

    def __repr__(self):
        return "<Item: {}/{} {}>".format(self.part, self.area, self.url)

    def _get_per_m2(self, html):
        rmb = html.select("body > div.overview > div.content > div.price > div.text > div.unitPrice > span")[0].text
        m = re.match("(\d+)元/平米", rmb)
        return float(m.group(1))

    def _get_area(self, area):
        m = re.match("([\d\.]+)㎡", area)
        return float(m.group(1)) if m else None

    def _construct_date(self, html):
        desc = html.select("body > div.overview > div.content > div.houseInfo > div.area > div.subInfo")[0].text
        m = re.match("(\d+)年建/\w+", desc)
        return float(m.group(1)) if m else None

    def _extract(self):
        html = get_soup(self.url)

        obj = {}
        obj["identity"] = self.url

        # money
        obj["finance"] = {}
        obj["finance"]["total"] = float(html.select("body > div.overview > div.content > div.price > span.total")[0].text) * 1e4
        obj["finance"]["down_payment"] = float('nan')
        obj["finance"]["tax"] = float('nan')
        obj["finance"]["per_m2"] = self._get_per_m2(html)

        #location
        obj["location"] = {}
        obj["location"]["name"] = html.select("body > div.overview > div.content > div.aroundInfo > div.communityName > a.info")[0].text
        obj["location"]["partition"] = self.part
        obj["location"]["area"] = self.area
        obj["location"]["supplement"] = [a.text for a in html.select("body > div.overview > div.content > div.aroundInfo > div.areaName > a")]


        # property
        obj["property"] = {}
        prop = defaultdict(lambda: None)
        prop.update({li.select('span')[0].text: li.find(text=True, recursive=False) for li in html.select("#introduction > div > div > div.base > div.content > ul > li")})
        obj["property"]["formation"] = prop["房屋户型"]
        obj["property"]["floor"] = prop["所在楼层"]
        obj["property"]["total_area"] = self._get_area(prop["建筑面积"])
        obj["property"]["construct_type"] = prop["建筑类型"]
        obj["property"]["structure"] = prop["建筑结构"]
        obj["property"]["orientation"] = prop["房屋朝向"]
        obj["property"]["construct_date"] = self._construct_date(html)

        return obj


class PageSpider:
    def __init__(self, cfg, base_url, url, part, name):
        self.cfg = cfg
        self.concurrency = cfg["concurrency"]["item"]
        self.base_url = base_url
        self.url = url
        self.part = part
        self.name = name

    def start_sync(self):
        links = self._get_links()

        jobs = [ItemSpider(self.cfg, link, self.part, self.name) for link in links]
        return jobs

    def _get_links(self):
        html = get_soup(self.url)
        divs = html.select("body > div.content > div.leftContent > ul > li > div.info.clear > div.title > a")
        return [div["href"] for div in divs]

    def __repr__(self):
        return "<Area: {}/{} @ {}>".format(self.part, self.name, self.url)


class AreaSpider:
    def __init__(self, cfg, base_url, url, part, name):
        self.cfg = cfg
        self.concurrency = cfg["concurrency"]["page"]
        self.base_url = base_url
        self.url = url
        self.part = part
        self.name = name

    def start_sync(self):
        total_item, page_links = self._generate_count_and_pages()

        jobs = [PageSpider(self.cfg, self.base_url, url, self.part, self.name) for url in page_links]
        success, failure, result = post_jobs(jobs, self.concurrency)

        print("{} total: {}, actual: {}, pages: {}".format(self, total_item, len(result), len(page_links)))
        return result

    def _generate_count_and_pages(self):
        html = get_soup(self.url)
        return self._get_count(html), self._get_pages(html)

    def _get_count(self, html):
        div = html.select("body > div.content > div.leftContent > div.resultDes > h2 > span")
        assert len(div) == 1, "should be only one item count number"
        count = int(div[0].text)
        if count == 0:
            raise KnownError("the page doesn't have any data")
        return count

    def _get_pages(self, html):
        obj = json.loads(html.select("body > div.content div.leftContent div.contentBottom div.page-box div")[0]['page-data'])

        assert 'totalPage' in obj, "should have total page"
        return [urljoin(self.url, "pg{}".format(idx + 1)) for idx in range(obj['totalPage'])]

    def __repr__(self):
        return "<Area: {}/{} @ {}>".format(self.part, self.name, self.url)


class SpiderLianJia:
    def __init__(self, cfg):
        self.cfg = cfg
        self.client = cfg["mongo"]
        self.db = cfg["mongo"]["data"]
        self.start_url = cfg["base_url"]
        self.concurrency = cfg["concurrency"]["area"]
        parsed = urlparse(self.start_url)
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"

    def start_sync(self):
        links = self.get_link_list()
        self.download_all(links)

    def download_all(self, links):
        concurrency = cfg["concurrency"]["page"]
        post_jobs(links, concurrency)

    def get_link_list(self):
        links = self._try_get_cached_links()
        if not links:
            links = self._download_link_list()
            self._cache_links(links)
        return links

    def _try_get_cached_links(self):
        cached_links = [ItemSpider(self.cfg, item["url"], item["part"], item["area"]) for item in self.db["cached_lianjia_links_"].find()]
        print("try get {} cached links".format(len(cached_links)))
        return cached_links


    def _cache_links(self, links):
        links = [{"url": l.url, "part": l.part, "area": l.area} for l in links]
        print("caching {} item links".format(len(links)))
        self.db["tmp_lianjia_links_"].drop()
        self.db["tmp_lianjia_links_"].insert_many(links)


    def _download_link_list(self):
        links = self._get_area_links(self.start_url)
        print("done generate area list ({} areas)".format(len(links)))
        jobs = [AreaSpider(self.cfg, self.base_url, url, *links[url]) for url in links]
        _, _, result = post_jobs(jobs, self.concurrency)
        return result

    def _get_area_links(self, start_url):
        all_urls = {}

        level1_url_names = self._get_level1_links(start_url)
        for url, name in level1_url_names:
            rs = self._get_level2_links(name, url)
            all_urls.update(rs)
        return all_urls

    def _get_level2_links(self, part, start_url):
        rs = {}

        top_level = get_soup(start_url)
        for second_level in top_level.select("body div.m-filter div.position > dl > dd > div")[0].select("div")[1].select("a"):
            rs[self.base_url + second_level["href"]] = (part, second_level.text)
        return rs

    def _get_level1_links(self, start_url):
        rs = []

        top_level = get_soup(start_url)
        for second_level in top_level.select("body div.m-filter div.position > dl > dd > div")[0].select("div > a"):
            rs.append((self.base_url + second_level["href"], second_level.text))
        return rs


if __name__ == "__main__":
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        cfg_file = sys.argv[1]
    else:
        cfg_file = "config.json"

    with open(cfg_file, "r") as file:
        cfg = json.load(file)

        cfg["ts"] = datetime.datetime.utcnow()
        Mongo.setup_client(cfg)

        print("config file content: {}".format(cfg))
        spider = SpiderLianJia(cfg)
        spider.start_sync()

        # item = ItemSpider(cfg, "https://sh.lianjia.com/ershoufang/107101026757.html", "part", "area")
        # print(item._extract())
