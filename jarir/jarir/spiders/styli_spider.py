import json
import multiprocessing
import traceback
from pathlib import Path
from urllib.parse import urljoin
import scrapy
from scrapling.fetchers import StealthyFetcher


START_URL = (
    "https://stylishop.com/sa/en/list/women/"
    "context/up-to-80-percent-off-w"
)

BASE_URL = "https://stylishop.com"

# Listing page
PRODUCT_XPATH = "//a[contains(@href,'/sa/en/product-')]"
NEXT_BUTTON_XPATH = "//button[@aria-label='Next page']"

MAX_PAGES = 2
MAX_SCROLLS = 5
SCROLL_WAIT = 3000

# Product details
PRODUCT_NAME_XPATH = "//div[contains(@class,'fs-16')]//h1"

CURRENT_PRICE_XPATH = (
    "(//div[contains(@class,'fw-6 c-dark-gray middle-xs')]"
    "//div[@class='d-il-block'])[1]"
)

OLD_PRICE_XPATH = (
    "(//div[contains(@class,'fw-6 c-dark-gray middle-xs')]"
    "//del)[1]"
)

DISCOUNT_XPATH = "(//span[contains(@class,'c-red-1')])[1]"

IMAGE_XPATH = (
    "(//img[contains(@class,'pdp-image-aspect-ratio')])[1]"
)

OUTPUT_DIR = Path.cwd()
URLS_FILE = OUTPUT_DIR / "styli_products_urls.json"
PRODUCTS_FILE = OUTPUT_DIR / "styli_products.json"


# ==================================================
# SCRAPY SPIDER CLASS
# ==================================================

class StyliSpider(scrapy.Spider):
    name = "styli"
    start_urls = [START_URL]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.all_product_urls = []
        self.products = []
        self.failed_products = 0

    def save_urls(self):
        """Save collected product URLs to JSON."""

        with URLS_FILE.open("w", encoding="utf-8") as file:
            json.dump(
                self.all_product_urls,
                file,
                indent=4,
                ensure_ascii=False,
            )

    def save_products(self):
        """Save scraped product details to JSON."""

        with PRODUCTS_FILE.open("w", encoding="utf-8") as file:
            json.dump(
                self.products,
                file,
                indent=4,
                ensure_ascii=False,
            )

    async def start(self):
        """
        Collect listing URLs and scrape product details
        without sending ordinary Scrapy requests to Styli.
        """

        self.logger.info("Opening Styli sale page in Chromium...")

        try:
            result = run_browser_task("listing", START_URL)

            self.all_product_urls = result["urls"]
            self.save_urls()

        except Exception:
            self.logger.exception(
                "Failed to load Styli listing page"
            )
            return

        total = len(self.all_product_urls)

        self.logger.info(
            "Total unique product URLs: %s",
            total,
        )

        if not self.all_product_urls:
            self.logger.warning(
                "No product URLs found. Check the listing XPath."
            )
            return

        # Scrape product details one by one.
        for index, product_url in enumerate(
            self.all_product_urls,
            start=1,
        ):
            self.logger.info(
                "========== PRODUCT %s/%s ==========",
                index,
                total,
            )
            self.logger.info("Product URL: %s", product_url)

            try:
                result = run_browser_task(
                    "product",
                    product_url,
                )

                product = result["product"]

                self.products.append(product)
                self.save_products()

                self.logger.info(
                    "Saved product: %s",
                    product.get("product_name"),
                )
                self.logger.info(
                    "Current price: %s",
                    product.get("current_price"),
                )

                yield product

            except Exception:
                self.failed_products += 1

                self.logger.exception(
                    "Failed to scrape product: %s",
                    product_url,
                )

    def closed(self, reason):
        """Save final data and print the scraping summary."""

        self.save_urls()
        self.save_products()

        self.logger.info(
            "========== STYLI SCRAPING COMPLETE =========="
        )
        self.logger.info(
            "Total URLs found: %s",
            len(self.all_product_urls),
        )
        self.logger.info(
            "Successfully scraped: %s",
            len(self.products),
        )
        self.logger.info(
            "Failed products: %s",
            self.failed_products,
        )
        self.logger.info(
            "Product URLs saved to: %s",
            URLS_FILE.resolve(),
        )
        self.logger.info(
            "Product details saved to: %s",
            PRODUCTS_FILE.resolve(),
        )
        self.logger.info("Close reason: %s", reason)


# ==================================================
# BROWSER WORKER
# ==================================================

def browser_worker(connection, task, url):
    """
    Run the synchronous Scrapling browser in a separate
    process to avoid Windows asyncio subprocess conflicts.
    """

    try:
        if task == "listing":
            collected_urls = []

            def page_action(page):
                nonlocal collected_urls
                collected_urls = scrape_all_pages(page)

            response = StealthyFetcher.fetch(
                url,
                headless=False,
                load_dom=True,
                network_idle=True,
                solve_cloudflare=True,
                real_chrome=False,
                timeout=120000,
                wait=5000,
                page_action=page_action,
            )

            if response.status != 200:
                connection.send({
                    "ok": False,
                    "error": (
                        f"Listing page returned HTTP {response.status}"
                    ),
                })
                return

            connection.send({
                "ok": True,
                "urls": collected_urls,
            })

        elif task == "product":
            scraped_product = None

            def page_action(page):
                nonlocal scraped_product
                scraped_product = scrape_product(page, url)

            response = StealthyFetcher.fetch(
                url,
                headless=True,
                load_dom=True,
                network_idle=True,
                timeout=120000,
                wait=1500,
                page_action=page_action,
            )

            if response.status != 200:
                connection.send({
                    "ok": False,
                    "error": (
                        f"Product page returned HTTP {response.status}"
                    ),
                })
                return

            if scraped_product is None:
                connection.send({
                    "ok": False,
                    "error": "Product extraction returned no result",
                })
                return

            connection.send({
                "ok": True,
                "product": scraped_product,
            })

    except Exception:
        connection.send({
            "ok": False,
            "error": traceback.format_exc(),
        })

    finally:
        connection.close()


def run_browser_task(task, url, timeout=300):
    """Start a browser process and receive its result."""

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
                "Browser worker exited with code "
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


# ==================================================
# LISTING PAGE FUNCTIONS
# ==================================================

def load_products(page):
    """Scroll the listing page to trigger lazy loading."""

    for scroll in range(1, MAX_SCROLLS + 1):
        page.evaluate(
            "window.scrollTo(0, document.body.scrollHeight)"
        )

        page.wait_for_timeout(SCROLL_WAIT)

        print(
            f"Scroll {scroll}/{MAX_SCROLLS} completed."
        )

    page.evaluate("window.scrollTo(0, 0)")


def get_product_urls(page):
    """Collect unique product URLs from the current page."""

    product_links = page.locator(
        f"xpath={PRODUCT_XPATH}"
    ).all()

    product_urls = []

    for link in product_links:
        href = link.get_attribute("href")

        if not href:
            continue

        product_url = urljoin(BASE_URL, href.strip())

        if product_url not in product_urls:
            product_urls.append(product_url)

    return product_urls


def load_and_retry(page):
    """Load products and retry once if none are found."""

    print("Loading products...")

    load_products(page)
    product_urls = get_product_urls(page)

    print("Products found:", len(product_urls))

    if not product_urls:
        print("No products found. Reloading page...")

        page.reload(
            wait_until="domcontentloaded",
            timeout=120000,
        )

        load_products(page)
        product_urls = get_product_urls(page)

        print(
            "Products found after reload:",
            len(product_urls),
        )

    return product_urls


def scrape_all_pages(page):
    """Collect unique product URLs across listing pages."""

    all_product_urls = []

    for page_number in range(1, MAX_PAGES + 1):
        print(
            f"\n========== PAGE {page_number} =========="
        )

        page_products = load_and_retry(page)

        if not page_products:
            print("No products found. Stopping pagination.")
            break

        new_products = 0

        for product_url in page_products:
            if product_url not in all_product_urls:
                all_product_urls.append(product_url)
                new_products += 1

        print("New products:", new_products)
        print("Total unique products:", len(all_product_urls))

        if page_number >= MAX_PAGES:
            print("Maximum page limit reached.")
            break

        next_button = page.locator(
            f"xpath={NEXT_BUTTON_XPATH}"
        )

        if next_button.count() == 0:
            print("Next page button not found.")
            break

        if not next_button.first.is_visible():
            print("Next page button is not visible.")
            break

        if next_button.first.is_disabled():
            print("Next page button is disabled.")
            break

        print("Clicking Next page...")

        next_button.first.click()
        page.wait_for_timeout(2000)

        print("Next page clicked.")

    return all_product_urls


# ==================================================
# PRODUCT DETAIL FUNCTIONS
# ==================================================

def get_text(page, xpath):
    """Extract text from the first matching element."""

    element = page.locator(
        f"xpath={xpath}"
    ).first

    try:
        element.wait_for(
            state="visible",
            timeout=30000,
        )

        text = element.text_content()

        if text:
            text = text.replace("\xa0", " ")
            text = text.strip()
            text = " ".join(text.split())
            return text

    except Exception as error:
        print("Text extraction error:", error)

    return None


def get_attribute(page, xpath, attribute):
    """Extract an attribute from the first matching element."""

    element = page.locator(
        f"xpath={xpath}"
    ).first

    try:
        element.wait_for(
            state="attached",
            timeout=30000,
        )

        value = element.get_attribute(attribute)

        if value:
            return value.strip()

    except Exception as error:
        print("Attribute extraction error:", error)

    return None


def scrape_product(page, product_url):
    """Open a product page and extract its details."""

    print("\nScraping product:")
    print(product_url)

    try:
        page.goto(
            product_url,
            wait_until="load",
            timeout=120000,
        )

        print("Product page loaded.")

        product_name = get_text(
            page,
            PRODUCT_NAME_XPATH,
        )

        current_price = get_text(
            page,
            CURRENT_PRICE_XPATH,
        )

        old_price = get_text(
            page,
            OLD_PRICE_XPATH,
        )

        discount = get_text(
            page,
            DISCOUNT_XPATH,
        )

        image_url = get_attribute(
            page,
            IMAGE_XPATH,
            "src",
        )

        if image_url:
            image_url = urljoin(product_url, image_url)

        product = {
            "product_url": product_url,
            "product_name": product_name,
            "current_price": current_price,
            "old_price": old_price,
            "discount": discount,
            "image_url": image_url,
        }

        print("Name:", product_name)
        print("Current price:", current_price)
        print("Old price:", old_price)
        print("Discount:", discount)
        print("Image:", image_url)

        return product

    except Exception as error:
        print("Error scraping product:", error)

        return {
            "product_url": product_url,
            "product_name": None,
            "current_price": None,
            "old_price": None,
            "discount": None,
            "image_url": None,
        }
