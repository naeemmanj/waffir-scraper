import json
import multiprocessing
import traceback
from pathlib import Path
from urllib.parse import urljoin

import scrapy
from scrapling.fetchers import StealthyFetcher


START_URL = "https://en-saudi.ounass.com/men/sale"
BASE_URL = "https://en-saudi.ounass.com"

MAX_SCROLLS = 20
SCROLL_WAIT = 3000

OUTPUT_DIR = Path.cwd()
URLS_FILE = OUTPUT_DIR / "ounass_products_urls.json"
PRODUCTS_FILE = OUTPUT_DIR / "ounass_products.json"


# ==================================================
# 1. CLASS FIRST
# ==================================================

class OunassSpider(scrapy.Spider):

    name = "pakistan"
    start_urls = [START_URL]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.all_product_urls = set()
        self.products = []
        self.failed_products = 0

    def run_browser_task(self, task, url, timeout=300):
        context = multiprocessing.get_context("spawn")
        parent_connection, child_connection = context.Pipe(
            duplex=False
        )

        process = context.Process(
            target=browser_worker,
            args=(child_connection, task, url),
        )

        try:
            process.start()
            child_connection.close()

            if not parent_connection.poll(timeout):
                process.terminate()
                process.join()

                raise TimeoutError(
                    f"Browser task '{task}' exceeded "
                    f"{timeout} seconds: {url}"
                )

            result = parent_connection.recv()
            process.join()

            if process.exitcode != 0:
                raise RuntimeError(
                    f"Browser worker exited with code "
                    f"{process.exitcode}"
                )

            if not result.get("ok"):
                raise RuntimeError(
                    result.get("error", "Unknown browser error")
                )

            return result

        finally:
            parent_connection.close()

            if process.is_alive():
                process.terminate()
                process.join()

    def save_urls(self):
        with URLS_FILE.open("w", encoding="utf-8") as file:
            json.dump(
                sorted(self.all_product_urls),
                file,
                indent=4,
                ensure_ascii=False,
            )

    def save_products(self):
        with PRODUCTS_FILE.open("w", encoding="utf-8") as file:
            json.dump(
                self.products,
                file,
                indent=4,
                ensure_ascii=False,
            )

    async def start(self):
        self.logger.info(
            "Opening Ounass men's sale page in Chromium..."
        )

        try:
            result = self.run_browser_task("listing", START_URL)
            self.all_product_urls.update(result["urls"])
            self.save_urls()

            self.logger.info(
                "Total unique product URLs found: %s",
                len(self.all_product_urls),
            )

        except Exception:
            self.logger.exception(
                "Failed to load Ounass sale page"
            )
            return

        if not self.all_product_urls:
            self.logger.warning(
                "No product URLs found. Check the page and XPath."
            )
            return

        total = len(self.all_product_urls)

        for index, product_url in enumerate(
            sorted(self.all_product_urls), start=1
        ):
            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product,
                errback=self.product_error,
                dont_filter=True,
                meta={
                    "product_url": product_url,
                    "product_index": index,
                    "product_total": total,
                },
            )

    def parse_product(self, response):
        product_url = response.meta["product_url"]

        self.logger.info(
            "Scraping product %s/%s: %s",
            response.meta["product_index"],
            response.meta["product_total"],
            product_url,
        )

        try:
            result = self.run_browser_task("product", product_url)
            product = result["product"]

            self.products.append(product)
            self.save_products()

            self.logger.info(
                "Saved product: %s | Price: %s",
                product.get("product_name"),
                product.get("current_price"),
            )

            yield product

        except Exception:
            self.failed_products += 1
            self.logger.exception(
                "Failed to scrape product: %s",
                product_url,
            )

    def product_error(self, failure):
        product_url = failure.request.meta.get(
            "product_url",
            failure.request.url,
        )

        self.failed_products += 1

        self.logger.error(
            "Scrapy request failed for %s: %s",
            product_url,
            failure.value,
        )

    def closed(self, reason):
        self.save_urls()
        self.save_products()

        self.logger.info("========== OUNASS SCRAPING COMPLETE ==========")
        self.logger.info("Total URLs found: %s", len(self.all_product_urls))
        self.logger.info("Successfully scraped: %s", len(self.products))
        self.logger.info("Failed or skipped: %s", self.failed_products)
        self.logger.info("Product URLs: %s", URLS_FILE.resolve())
        self.logger.info("Product details: %s", PRODUCTS_FILE.resolve())
        self.logger.info("Close reason: %s", reason)


# ==================================================
# 2. ALL HELPER FUNCTIONS AFTER THE CLASS
# ==================================================

def scroll_page(page):
    """Scroll the sale page and allow more products to load."""

    for scroll in range(1, MAX_SCROLLS + 1):
        links = page.locator(
            "xpath=//li[contains(@class,'StyleColorListItem')]/a"
        ).all()

        print(
            f"Scroll {scroll}/{MAX_SCROLLS}: "
            f"{len(links)} product links currently visible"
        )

        page.evaluate("""
            window.scrollBy({
                top: window.innerHeight * 5,
                behavior: 'instant'
            });
        """)

        page.wait_for_timeout(SCROLL_WAIT)


def get_text(response, xpath):
    """Return the first non-empty text matching an XPath."""

    values = response.xpath(xpath).getall()

    for value in values:
        value = value.strip()

        if value:
            return value

    return None


def browser_worker(connection, task, url):
    """Run browser scraping in a separate Windows process."""

    try:
        if task == "listing":
            response = StealthyFetcher.fetch(
                url,
                headless=False,
                load_dom=True,
                network_idle=True,
                wait=3000,
                timeout=60000,
                page_action=scroll_page,
            )

            if response.status != 200:
                connection.send({
                    "ok": False,
                    "error": f"Listing page returned HTTP {response.status}",
                })
                return

            hrefs = response.xpath(
                "//li[contains(@class,'StyleColorListItem')]/a/@href"
            ).getall()

            product_urls = list({
                urljoin(BASE_URL, href.strip())
                for href in hrefs
                if href and href.strip()
            })

            connection.send({
                "ok": True,
                "status": response.status,
                "urls": product_urls,
            })

        elif task == "product":
            response = StealthyFetcher.fetch(
                url,
                headless=True,
                load_dom=True,
                network_idle=True,
                wait=1500,
                timeout=60000,
            )

            if response.status != 200:
                connection.send({
                    "ok": False,
                    "error": f"Product page returned HTTP {response.status}",
                })
                return

            image_url = get_text(
                response,
                "//meta[@property='og:image']/@content",
            )

            if image_url:
                image_url = urljoin(url, image_url)

            product = {
                "product_url": url,
                "product_name": get_text(
                    response,
                    "//h1[contains(@class,'PDPDesktop-name')]//text()",
                ),
                "current_price": get_text(
                    response,
                    "//div[contains(@class,'PriceContainer')]"
                    "//span[contains(@class,'PriceContainer-price')]/text()",
                ),
                "old_price": get_text(
                    response,
                    "//div[contains(@class,'PriceContainer')]"
                    "//span[contains(@class,'PriceContainer-slashedPrice')]/text()",
                ),
                "discount": get_text(
                    response,
                    "//span[contains(@class,'PriceContainer-discountPercent')]/text()",
                ),
                "image_url": image_url,
            }

            connection.send({
                "ok": True,
                "product": product,
            })

        else:
            connection.send({
                "ok": False,
                "error": f"Unknown browser task: {task}",
            })

    except Exception:
        connection.send({
            "ok": False,
            "error": traceback.format_exc(),
        })

    finally:
        connection.close()