import os
import time

import scrapy
from scrapy import signals
from scrapy.settings.default_settings import RETRY_HTTP_CODES


class BolSpider(scrapy.Spider):
    name = "custom/bol.com"
    allowed_domains = ['bol.com']
    web_url = 'https://www.bol.com'
    item_count = 0

    # headers = {
    #     'Host': 'www.bol.com',
    #     'Accept': '*/*',
    #     'Accept-Language': 'nl-NL',
    #     'Accept-Encoding': 'gzip, deflate, br',
    #     'Content-Type': 'application/json, text/plain, */*',
    #     'X-Requested-With': 'XMLHttpRequest',
    #     'Connection': 'keep-alive',
    #     'Sec-Fetch-Dest': 'empty',
    #     'Sec-Fetch-Mode': 'cors',
    #     'Sec-Fetch-Site': 'same-origin'
    # }

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 0.3,
        'ROBOTSTXT_OBEY': True,
        'COOKIES_ENABLED': False,

        'FEED_FORMAT': 'csv',

        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:97.0) Gecko/20100101 Firefox/97.0',

        'RETRY_TIMES': 15,
        'RETRY_HTTP_CODES': RETRY_HTTP_CODES + [400, 401, 403, 404, 408, 409, 410, 411, 413, 414, 416, 417, 421, 422,
                                                423, 425, 426, 428, 429, 500, 502, 503, 504, 505, 506, 507, 508, 511],
        # extend retry http codes

    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Get parameters for the scraper
        if os.environ.get('EXTERNAL_ARGS'):
            self.urls = os.environ.get('EXTERNAL_ARGS')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BolSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        spider.crawler = crawler
        return spider

    def start_requests(self):
        # The scraper can work with urls list in the form of url1,url2,url3,etc
        for url in self.urls.split(','):
            yield scrapy.Request(url, callback=self.parse_categories, dont_filter=True)

    def parse_categories(self, response):

        if response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text"):
            # if page_type_check is int, the page is displaying products
            if int(response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text").get()) > 0:
                max_page_number = int(response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text").get()) + 1
            else:
                max_page_number = 1
            for page in range(1, max_page_number):
                if response.url.rsplit('?'):
                    current_page = response.url.rsplit('?')[0] + f'?page={page}'
                    yield response.follow(current_page, callback=self.parse_details, dont_filter=True)
                else:
                    current_page = response.url + f'?page={page}'
                    yield scrapy.Request(current_page, callback=self.parse_details, dont_filter=True)
            else:
                # if there are subcategories on the page navigate them
                if response.css("a.px_listpage_categoriesleft_click::attr(href)"):
                    for link in response.css("a.px_listpage_categoriesleft_click::attr(href)").getall():
                        url = self.web_url + link
                        yield scrapy.Request(url, callback=self.parse_categories, dont_filter=True)

        elif response.css("a.link-cta::attr(href)"):
            url = self.web_url + response.css("a.link-cta::attr(href)").get()
            yield scrapy.Request(url, callback=self.parse_categories, dont_filter=True)

        elif response.css("#doormat a::attr(href)"):
            # the #doormat css is for a specific type of page like 'https://www.bol.com/nl/nl/m/electronics/'
            for subcategory in response.css('#doormat a::attr(href)').getall():
                subcategory_url = 'https:' + subcategory
                yield scrapy.Request(subcategory_url, callback=self.parse_categories, dont_filter=True)

        elif not response.css("a.px_listpage_categoriesleft_click::attr(href)"):
            # if there are no more subcategories, go to parse_products
            if response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text"):
                if int(response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text").get()) > 0:
                    max_page_number = int(
                        response.css(".pagination > li:nth-child(7) > a:nth-child(1)::text").get()) + 1
                else:
                    max_page_number = 1
                for page in range(1, max_page_number):
                    if response.url.rsplit('?'):
                        current_page = response.url.rsplit('?')[0] + f'?page={page}'
                        yield response.follow(current_page, callback=self.parse_details, dont_filter=True)

        elif not response.css("a.px_listpage_categoriesleft_click::attr(href)") and not response.css(
                ".pagination > li:nth-child(7) > a:nth-child(1)::text"):
            # if the page doesn't contain subcategories , neither pagination, go to parse_products
            yield scrapy.Request(response.url, callback=self.parse_details, dont_filter=True)

    def item_scraped(self):
        self.item_count += 1
        if self.item_count % 7131 == 0:
            self.logger.info(f"Pausing scrape job, I got tired, I already scraped {self.item_count}")
            self.crawler.engine.pause()
            time.sleep(10)
            self.crawler.engine.unpause()
            self.logger.info(f"Back to work chief, full speed ahead ! ")
        elif self.item_count % 12111 == 0:
            self.logger.info(f"Pausing scrape job, I got tired, I already scraped {self.item_count}")
            self.crawler.engine.pause()
            time.sleep(30)
            self.crawler.engine.unpause()
            self.logger.info(f"Back to work chief, full speed ahead ! ")
        elif self.item_count % 50131 == 0:
            self.logger.info(f"Pausing scrape job, I got tired, I already scraped {self.item_count}")
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            self.logger.info(f"Back to work chief, full speed ahead ! ")
        elif self.item_count % 110017 == 0:
            self.logger.info(f"Pausing scrape job, I got tired, I already scraped {self.item_count}")
            self.crawler.engine.pause()
            time.sleep(120)
            self.crawler.engine.unpause()
            self.logger.info(f"Back to work chief, full speed ahead ! ")
        elif self.item_count % 500009 == 0:
            self.logger.info(f"Pausing scrape job, I got tired, I already scraped {self.item_count}")
            self.crawler.engine.pause()
            time.sleep(240)
            self.crawler.engine.unpause()
            self.logger.info(f"Back to work chief, full speed ahead ! ")

    def parse_details(self, response):
        if response.css('li.breadcrumbs__item:nth-child(2) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_1 = response.css(
                'li.breadcrumbs__item:nth-child(2) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_1 = ''
        if response.css('li.breadcrumbs__item:nth-child(3) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_2 = response.css(
                'li.breadcrumbs__item:nth-child(3) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_2 = ''
        if response.css('li.breadcrumbs__item:nth-child(4) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_3 = response.css(
                'li.breadcrumbs__item:nth-child(4) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_3 = ''
        if response.css('li.breadcrumbs__item:nth-child(5) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_4 = response.css(
                'li.breadcrumbs__item:nth-child(5) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_4 = ''
        if response.css('li.breadcrumbs__item:nth-child(6) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_5 = response.css(
                'li.breadcrumbs__item:nth-child(6) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_5 = ''
        product_item_type = 'column'
        # some categories are using columns other are using rows
        if response.css('li.product-item--column'):
            product_item_type = 'column'
        elif response.css('li.product-item--row'):
            product_item_type = 'row'

        for product in response.css('li.product-item--' + f'{product_item_type}'):
            description = ''
            name = ''
            brand = ''
            url = self.web_url + product.css('a.product-title::attr(href)').get()
            'li.product-item--column:nth-child(1) > div:nth-child(3) > a:nth-child(3) > span:nth-child(1)'
            if product.css('span.truncate::text'):
                name = product.css('span.truncate::text').get()
            elif product.css('a.product-title::text'):
                name = product.css('a.product-title::text').get().strip()
            if product.css("ul.product-creator > li:nth-child(1) > a:nth-child(1)::text"):
                brand = product.css("ul.product-creator > li:nth-child(1) > a:nth-child(1)::text").get().strip()
            elif name:
                brand = name.split()[0]
            product_id = url.rsplit('/')[-2]
            if product.css("div.product-subtitle::text"):
                description = product.css("div.product-subtitle::text").get().strip()
            if product.css("span.promo-price::text"):
                int_price = product.css("span.promo-price::text").get().strip()
            else:
                int_price = ''
            if product.css("sup.promo-price__fraction::text"):
                fraction_price = product.css("sup.promo-price__fraction::text").get().strip()
                if fraction_price == '-':
                    fraction_price = ''
            else:
                fraction_price = ''
            price = int_price + '.' + fraction_price

            product = {
                'shop': 'bol.com',
                'product_id': product_id,
                'product_name': name,
                'brand': brand,
                'price': price,
                'category_level_1': category_level_1,
                'category_level_2': category_level_2,
                'category_level_3': category_level_3,
                'category_level_4': category_level_4,
                'category_level_5': category_level_5,
                'description': description,
                'url': url

            }

            yield product
