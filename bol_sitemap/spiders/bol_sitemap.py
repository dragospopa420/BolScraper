import time
import scrapy
from scrapy import signals
from scrapy.settings.default_settings import RETRY_HTTP_CODES
from scrapy.spiders import SitemapSpider


class BolSpider(SitemapSpider):
    name = "sitemap/bol.com"
    allowed_domains = ['bol.com']
    web_url = 'https://www.bol.com'
    sitemap_urls = ['https://sitemap.bol.com/v0.9/index']
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

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BolSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        spider.crawler = crawler
        return spider

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

    def parse(self, response):
        if response.css('li.breadcrumbs__item:nth-child(2) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_1 = response.css('li.breadcrumbs__item:nth-child(2) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_1 = ''
        if response.css('li.breadcrumbs__item:nth-child(3) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_2 = response.css('li.breadcrumbs__item:nth-child(3) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_2 = ''
        if response.css('li.breadcrumbs__item:nth-child(4) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_3 = response.css('li.breadcrumbs__item:nth-child(4) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_3 = ''
        if response.css('li.breadcrumbs__item:nth-child(5) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_4 = response.css('li.breadcrumbs__item:nth-child(5) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_4 = ''
        if response.css('li.breadcrumbs__item:nth-child(6) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)'):
            category_level_5 = response.css('li.breadcrumbs__item:nth-child(6) > span:nth-child(1) > a:nth-child(1) > p:nth-child(1)::text').get().strip()
        else:
            category_level_5 = ''

        description = ''
        if response.css('.page-heading > span:nth-child(1)::text'):
            name = response.css('.page-heading > span:nth-child(1)::text').get().strip()
        else:
            name = ''
        if response.css('div.pdp-header__meta-item:nth-child(1) > a:nth-child(1)::text'):
            brand = response.css('div.pdp-header__meta-item:nth-child(1) > a:nth-child(1)::text').get().strip()
        else:
            brand = ''
        product_id = response.url.rsplit('/')[-2]
        if response.css('.product-description'):
            description = response.css('.product-description::text').get().strip()
        else:
            description = ''
        if response.css('span.promo-price'):
            int_price = response.css('span.promo-price::text').get().strip()
        else:
            int_price = ''
        if response.css('.promo-price__fraction'):
            fraction_price = response.css('.promo-price__fraction::text').get().strip()
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
            'url': response.url

        }

        yield product
