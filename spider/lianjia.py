from urllib.parse import urlparse
from common.request import get_soup
from common.threaded_job import post_jobs


class AreaSpider:
    def __init__(self, base_url, url, name):
        self.base_url = base_url
        self.url = url
        self.name = name

    def start_sync(self):
        total_item, page_links = self._generate_count_and_pages()
        print("Area: {} total: {}, pages: {}".format(self.name, total_item, len(page_links)))

        # TODO deal with all pages

    def _generate_count_and_pages(self):
        html = get_soup(self.url)
        return self._get_count(html), self._get_pages(html)

    def _get_count(self, html):
        div = html.select("div.search-result > span.result-count")
        assert len(div) == 1, "should be only one item count number"
        return int(div[0].text)

    def _get_pages(self, html):
        div = html.select("div.content > div.m-list > div.c-pagination > a")

        last = None
        for item in div:
            if item["gahref"] == "results_totalpage":
                last = int(item.text)
        assert last, "last page should exist"
        return ["{}/d{}".format(self.url, idx + 1) for idx in range(last)]


class SpiderLianJia:
    def __init__(self, cfg):
        self.start_url = cfg["base_url"]
        self.concurrency = cfg["concurrency"]["area"]
        parsed = urlparse(self.start_url)
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"

    def start_sync(self):
        links = self._get_area_links(self.start_url)
        print("done generate area list ({} areas)".format(len(links)))

        jobs = [AreaSpider(self.base_url, url, links[url]) for url in links]
        post_jobs(jobs, self.concurrency)


    def _get_area_links(self, start_url):
        all_urls = {}

        level1_urls = self._get_level1_links(start_url)
        for url in level1_urls:
            rs = self._get_level2_links(url)
            all_urls.update(rs)
        return all_urls

    def _get_level2_links(self, start_url):
        rs = {}

        top_level = get_soup(start_url)
        for second_level in top_level.select("#plateList div.level2 > div.level2-item > a"):
            if "on" not in second_level["class"]:
                rs[self.base_url + second_level["href"]] = second_level.text
        return rs

    def _get_level1_links(self, start_url):
        rs = []

        top_level = get_soup(start_url)
        for second_level in top_level.select("#plateList div.level1 > a.level1-item"):
            if "on" not in second_level["class"]:
                rs.append(self.base_url + second_level["href"])
        return rs


if __name__ == "__main__":
    cfg = {"base_url": "http://sh.lianjia.com/ershoufang", "concurrency": {"area": 1, "page": 1}}

    spider = SpiderLianJia(cfg)
    spider.start_sync()

