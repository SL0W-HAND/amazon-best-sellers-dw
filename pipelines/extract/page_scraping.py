"""
Amazon Product Page Scraper
===========================
A modular web scraper for extracting product details from Amazon product pages.

Usage:
    from page_scraping import scrape_product_page, ProductDetails
    
    details = scrape_product_page("https://www.amazon.com/dp/B09FLNSYDZ")
    print(details.price, details.asin, details.brand)
    print(details.rating, details.total_reviews)
    for review in details.reviews:
        print(review)
"""

import re
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Review:
    """Represents a single customer review."""
    review_id: str
    stars: str
    date: str
    location: str
    title: str
    text: str
    verified_purchase: bool = False
    helpful_count: str = ''
    
    def __str__(self) -> str:
        return (
            f"Review ID: {self.review_id}\n"
            f"Stars: {self.stars}\n"
            f"Date: {self.date}\n"
            f"Location: {self.location}\n"
            f"Title: {self.title}\n"
            f"Text: {self.text}\n"
            f"Verified Purchase: {self.verified_purchase}\n"
            f"Helpful: {self.helpful_count}"
        )


@dataclass
class StarHistogram:
    """Represents the star rating distribution."""
    five_star: str = '0%'
    four_star: str = '0%'
    three_star: str = '0%'
    two_star: str = '0%'
    one_star: str = '0%'
    
    def __str__(self) -> str:
        return (
            f"5 stars: {self.five_star}\n"
            f"4 stars: {self.four_star}\n"
            f"3 stars: {self.three_star}\n"
            f"2 stars: {self.two_star}\n"
            f"1 star: {self.one_star}"
        )
    
    def to_dict(self) -> Dict[str, str]:
        return {
            '5_star': self.five_star,
            '4_star': self.four_star,
            '3_star': self.three_star,
            '2_star': self.two_star,
            '1_star': self.one_star,
        }


@dataclass
class ProductDetails:
    """Represents the extracted details from a product page."""
    price: str
    asin: str
    brand: str
    url: str
    rating: str = 'N/A'
    total_reviews: str = 'N/A'
    star_histogram: StarHistogram = field(default_factory=StarHistogram)
    reviews: List[Review] = field(default_factory=list)
    
    def __str__(self) -> str:
        reviews_summary = f"{len(self.reviews)} reviews extracted" if self.reviews else "No reviews"
        return (
            f"Price: {self.price}\n"
            f"ASIN: {self.asin}\n"
            f"Brand: {self.brand}\n"
            f"Rating: {self.rating}\n"
            f"Total Reviews: {self.total_reviews}\n"
            f"Star Distribution:\n{self.star_histogram}\n"
            f"Reviews: {reviews_summary}"
        )


@dataclass
class ProductScraperConfig:
    """Configuration settings for the product page scraper."""
    
    # Browser settings
    headless: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Timing
    page_load_wait_seconds: float = 1.0
    
    # Scrolling settings for loading lazy content (reviews)
    scroll_step_pixels: int = 800  # Pixels to scroll each step
    scroll_pause_seconds: float = 0.5  # Pause between scrolls
    lazy_load_wait_seconds: float = 2.0  # Wait for lazy content to load
    max_no_change_attempts: int = 3  # Stop after this many scrolls with no new reviews


# =============================================================================
# BROWSER DRIVER
# =============================================================================

class ChromeDriverFactory:
    """Factory for creating configured Chrome WebDriver instances."""
    
    @staticmethod
    def create(config: ProductScraperConfig) -> webdriver.Chrome:
        """Create and return a configured Chrome WebDriver."""
        chrome_options = Options()
        
        if config.headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(f'--user-agent={config.user_agent}')
        
        return webdriver.Chrome(options=chrome_options)


# =============================================================================
# SCROLL HANDLER
# =============================================================================

class ReviewScrollHandler:
    """Handles scrolling to trigger lazy-loaded reviews."""
    
    def __init__(self, driver: webdriver.Chrome, config: ProductScraperConfig):
        self.driver = driver
        self.config = config
    
    def scroll_until_all_loaded(self) -> int:
        """
        Scroll the page gradually to load all lazy-loaded reviews.
        
        Returns:
            The total number of reviews found after scrolling.
        """
        # First, scroll to the reviews section
        self._scroll_to_reviews_section()
        
        last_count = 0
        no_change_count = 0
        scroll_position = self._get_current_scroll_position()
        
        while no_change_count < self.config.max_no_change_attempts:
            page_height = self._get_page_height()
            scroll_position = self._scroll_down(scroll_position)
            
            if scroll_position >= page_height:
                current_count = self._wait_and_count_reviews()
                
                if current_count == last_count:
                    no_change_count += 1
                else:
                    no_change_count = 0
                    last_count = current_count
                
                scroll_position = self._update_scroll_position(scroll_position, page_height)
        
        # Scroll back to reviews section
        self._scroll_to_review_list()
        
        return last_count
    
    def _scroll_to_reviews_section(self) -> None:
        """Scroll to the reviews medley section."""
        try:
            self.driver.execute_script(
                "document.getElementById('reviewsMedley')?.scrollIntoView({behavior: 'smooth'});"
            )
            time.sleep(self.config.scroll_pause_seconds)
        except Exception:
            pass
    
    def _scroll_to_review_list(self) -> None:
        """Scroll to the review list element."""
        try:
            self.driver.execute_script(
                "document.getElementById('cm-cr-dp-review-list')?.scrollIntoView();"
            )
            time.sleep(self.config.scroll_pause_seconds)
        except Exception:
            pass
    
    def _get_page_height(self) -> int:
        """Get the current page height."""
        return self.driver.execute_script("return document.body.scrollHeight")
    
    def _get_current_scroll_position(self) -> int:
        """Get the current scroll position."""
        return self.driver.execute_script("return window.pageYOffset")
    
    def _scroll_down(self, current_position: int) -> int:
        """Scroll down by the configured step amount."""
        new_position = current_position + self.config.scroll_step_pixels
        self.driver.execute_script(f"window.scrollTo(0, {new_position});")
        time.sleep(self.config.scroll_pause_seconds)
        return new_position
    
    def _wait_and_count_reviews(self) -> int:
        """Wait for lazy loading and count reviews."""
        time.sleep(self.config.lazy_load_wait_seconds)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        review_list = soup.find('ul', {'id': 'cm-cr-dp-review-list'})
        if review_list:
            reviews = review_list.find_all('li', {'data-hook': 'review'})
            return len(reviews)
        return 0
    
    def _update_scroll_position(self, current_position: int, old_height: int) -> int:
        """Update scroll position if the page grew."""
        new_height = self._get_page_height()
        if new_height > old_height:
            return old_height
        return current_position


# =============================================================================
# PAGE PARSER
# =============================================================================

class ProductPageParser:
    """Parses Amazon product pages and extracts product information."""
    
    def __init__(self, soup: BeautifulSoup):
        self.soup = soup
    
    def extract_price(self) -> str:
        """Extract the product price."""
        try:
            whole = self.soup.find('span', {'class': 'a-price-whole'})
            fraction = self.soup.find('span', {'class': 'a-price-fraction'})
            
            if whole and fraction:
                return f"{whole.text.strip()}{fraction.text.strip()}"
            elif whole:
                return whole.text.strip()
        except Exception:
            pass
        
        return 'N/A'
    
    def extract_asin(self) -> str:
        """Extract the ASIN from the product details table."""
        # Method 1: Look for th with "ASIN" text
        asin_header = self.soup.find('th', string=lambda text: text and 'ASIN' in text.strip())
        if asin_header:
            asin_value = asin_header.find_next_sibling('td')
            if asin_value:
                return asin_value.text.strip()
        
        # Method 2: Look in product details table by class
        details_table = self.soup.find('table', {'id': 'productDetails_detailBullets_sections1'})
        if details_table:
            rows = details_table.find_all('tr')
            for row in rows:
                header = row.find('th')
                if header and 'ASIN' in header.text:
                    value = row.find('td')
                    if value:
                        return value.text.strip()
        
        return 'N/A'
    
    def extract_brand(self) -> str:
        """Extract the product brand."""
        # Method 1: Look for "Brand" in detail table (th/td structure)
        brand_header = self.soup.find('th', string=lambda text: text and 'Brand' in text.strip())
        if brand_header:
            brand_value = brand_header.find_next_sibling('td')
            if brand_value:
                return brand_value.text.strip()
        
        # Method 2: Look in product details bullets
        brand_row = self.soup.find('tr', class_='po-brand')
        if brand_row:
            value = brand_row.find('td', class_='a-span9')
            if value:
                return value.text.strip()
        
        # Method 3: Look for bylineInfo link (brand store link)
        byline = self.soup.find('a', {'id': 'bylineInfo'})
        if byline:
            brand_text = byline.text.strip()
            # Remove "Visit the X Store" or "Brand: X" prefix
            if 'Visit the' in brand_text:
                return brand_text.replace('Visit the', '').replace('Store', '').strip()
            if 'Brand:' in brand_text:
                return brand_text.replace('Brand:', '').strip()
            return brand_text
        
        return 'N/A'
    
    def extract_rating(self) -> str:
        """Extract the overall product rating."""
        try:
            # Method 1: Look for average star rating icon
            rating_icon = self.soup.find('i', {'data-hook': 'average-star-rating'})
            if rating_icon:
                alt_text = rating_icon.find('span', class_='a-icon-alt')
                if alt_text:
                    # Extract rating like "4.4 de 5" or "4.4 out of 5"
                    text = alt_text.text.strip()
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        return match.group(1)
            
            # Method 2: Look for rating text directly
            rating_text = self.soup.find('span', {'data-hook': 'rating-out-of-text'})
            if rating_text:
                text = rating_text.text.strip()
                match = re.search(r'(\d+\.?\d*)', text)
                if match:
                    return match.group(1)
            
            # Method 3: Look in the product summary area
            rating_span = self.soup.find('span', class_='a-icon-alt', string=re.compile(r'\d+\.?\d*.*estrellas|stars', re.I))
            if rating_span:
                match = re.search(r'(\d+\.?\d*)', rating_span.text)
                if match:
                    return match.group(1)
        except Exception:
            pass
        
        return 'N/A'
    
    def extract_total_reviews(self) -> str:
        """Extract the total number of reviews/ratings."""
        try:
            # Method 1: Look for total review count hook
            total_elem = self.soup.find('span', {'data-hook': 'total-review-count'})
            if total_elem:
                text = total_elem.text.strip()
                # Extract number (handle formats like "35,231" or "35.231")
                numbers = re.findall(r'[\d,\.]+', text)
                if numbers:
                    return numbers[0]
            
            # Method 2: Look for ratings count link
            ratings_link = self.soup.find('a', {'id': 'acrCustomerReviewLink'})
            if ratings_link:
                text = ratings_link.text.strip()
                numbers = re.findall(r'[\d,\.]+', text)
                if numbers:
                    return numbers[0]
        except Exception:
            pass
        
        return 'N/A'
    
    def extract_star_histogram(self) -> StarHistogram:
        """Extract the star rating distribution (histogram)."""
        histogram = StarHistogram()
        
        try:
            histogram_table = self.soup.find('ul', {'id': 'histogramTable'})
            if histogram_table:
                rows = histogram_table.find_all('li')
                star_mapping = {
                    0: 'five_star',
                    1: 'four_star', 
                    2: 'three_star',
                    3: 'two_star',
                    4: 'one_star'
                }
                
                for idx, row in enumerate(rows):
                    if idx > 4:
                        break
                    # Try to get percentage from meter
                    meter = row.find('div', {'role': 'progressbar'})
                    if meter and meter.get('aria-valuenow'):
                        percentage = f"{meter['aria-valuenow']}%"
                        attr_name = star_mapping.get(idx)
                        if attr_name:
                            setattr(histogram, attr_name, percentage)
                    else:
                        # Fallback: look for percentage text
                        text_spans = row.find_all('span', class_='_cr-ratings-histogram_style_histogram-column-space__RKUAd')
                        # The last span with percentage that's not hidden
                        link = row.find('a')
                        if link:
                            link_text = link.text.strip()
                            match = re.search(r'(\d+)%', link_text)
                            if match:
                                attr_name = star_mapping.get(idx)
                                if attr_name:
                                    setattr(histogram, attr_name, f"{match.group(1)}%")
        except Exception:
            pass
        
        return histogram
    
    def extract_reviews(self) -> List[Review]:
        """Extract all reviews from the page (local and global reviews)."""
        reviews = []
        seen_ids = set()  # To avoid duplicates
        
        try:
            # Find reviews from multiple containers:
            # 1. cm-cr-dp-review-list: Local reviews (from the same marketplace)
            # 2. cm-cr-global-review-list: Global reviews (from other regions)
            review_list_ids = ['cm-cr-dp-review-list', 'cm-cr-global-review-list']
            
            for list_id in review_list_ids:
                review_list = self.soup.find('ul', {'id': list_id})
                if review_list:
                    review_elements = review_list.find_all('li', {'data-hook': 'review'})
                    for review_elem in review_elements:
                        review_id = review_elem.get('id', '')
                        if review_id and review_id not in seen_ids:
                            seen_ids.add(review_id)
                            review = self._parse_single_review(review_elem)
                            if review:
                                reviews.append(review)
            
            # Fallback: if no reviews found in specific lists, search entire page
            if not reviews:
                review_elements = self.soup.find_all('li', {'data-hook': 'review'})
                for review_elem in review_elements:
                    review_id = review_elem.get('id', '')
                    if review_id and review_id not in seen_ids:
                        seen_ids.add(review_id)
                        review = self._parse_single_review(review_elem)
                        if review:
                            reviews.append(review)
        except Exception:
            pass
        
        return reviews
    
    def _parse_single_review(self, review_elem) -> Optional[Review]:
        """Parse a single review element."""
        try:
            # Review ID
            review_id = review_elem.get('id', 'N/A')
            
            # Stars
            stars = 'N/A'
            star_elem = review_elem.find('i', {'data-hook': 'review-star-rating'})
            if star_elem:
                alt_span = star_elem.find('span', class_='a-icon-alt')
                if alt_span:
                    text = alt_span.text.strip()
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        stars = match.group(1)
            
            # Date and Location
            date = 'N/A'
            location = 'N/A'
            date_elem = review_elem.find('span', {'data-hook': 'review-date'})
            if date_elem:
                date_text = date_elem.text.strip()
                # Parse date and location from text like:
                # "Calificado en Estados Unidos el 4 de enero de 2026"
                # "Reviewed in the United States on January 4, 2026"
                
                # Try to extract location
                location_patterns = [
                    r'(?:Calificado en|Reviewed in|Revisado en)\s+(.+?)\s+(?:el|on)',
                    r'(?:in|en)\s+(.+?)\s+(?:el|on)',
                ]
                for pattern in location_patterns:
                    match = re.search(pattern, date_text, re.I)
                    if match:
                        location = match.group(1).strip()
                        break
                
                # Try to extract date
                date_patterns = [
                    r'(?:el|on)\s+(.+)$',
                    r'\d{1,2}\s+(?:de\s+)?\w+\s+(?:de\s+)?\d{4}',
                    r'\w+\s+\d{1,2},?\s+\d{4}',
                ]
                for pattern in date_patterns:
                    match = re.search(pattern, date_text, re.I)
                    if match:
                        date = match.group(1).strip() if match.lastindex else match.group(0).strip()
                        break
            
            # Title
            title = 'N/A'
            title_elem = review_elem.find('a', {'data-hook': 'review-title'})
            if title_elem:
                # Get the title text (excluding the star rating)
                title_span = title_elem.find('span', class_='cr-original-review-content')
                if title_span:
                    title = title_span.text.strip()
                else:
                    # Fallback: get text from the link but skip star rating
                    for span in title_elem.find_all('span'):
                        if 'a-icon-alt' not in span.get('class', []):
                            text = span.text.strip()
                            if text and 'estrellas' not in text.lower() and 'stars' not in text.lower():
                                title = text
                                break
            
            # Review text/body
            text = 'N/A'
            body_elem = review_elem.find('span', {'data-hook': 'review-body'})
            if body_elem:
                # Look for the original content (not translated)
                original_content = body_elem.find('span', class_='cr-original-review-content')
                if original_content:
                    text = original_content.get_text(separator=' ', strip=True)
                else:
                    review_text_div = body_elem.find('div', class_='reviewText')
                    if review_text_div:
                        text = review_text_div.get_text(separator=' ', strip=True)
                    else:
                        text = body_elem.get_text(separator=' ', strip=True)
            
            # Verified purchase
            verified = False
            verified_elem = review_elem.find('span', {'data-hook': 'avp-badge-linkless'})
            if verified_elem:
                verified = True
            
            # Helpful count
            helpful = ''
            helpful_elem = review_elem.find('span', {'data-hook': 'helpful-vote-statement'})
            if helpful_elem:
                helpful = helpful_elem.text.strip()
            
            return Review(
                review_id=review_id,
                stars=stars,
                date=date,
                location=location,
                title=title,
                text=text,
                verified_purchase=verified,
                helpful_count=helpful
            )
        except Exception:
            return None
    
    def extract_all(self, url: str) -> ProductDetails:
        """Extract all product details."""
        return ProductDetails(
            price=self.extract_price(),
            asin=self.extract_asin(),
            brand=self.extract_brand(),
            url=url,
            rating=self.extract_rating(),
            total_reviews=self.extract_total_reviews(),
            star_histogram=self.extract_star_histogram(),
            reviews=self.extract_reviews(),
        )


# =============================================================================
# MAIN SCRAPER
# =============================================================================

class ProductPageScraper:
    """Main scraper class for product pages."""
    
    def __init__(self, config: Optional[ProductScraperConfig] = None):
        self.config = config or ProductScraperConfig()
        self.driver: Optional[webdriver.Chrome] = None
    
    def scrape(self, url: str) -> ProductDetails:
        """
        Scrape a product page and extract details.
        
        Args:
            url: The full URL of the Amazon product page
        
        Returns:
            ProductDetails containing price, ASIN, and brand
        """
        try:
            self._initialize_driver()
            self._navigate_to_page(url)
            self._scroll_to_load_reviews()
            return self._extract_details(url)
        finally:
            self._cleanup()
    
    def _initialize_driver(self) -> None:
        """Initialize the Chrome WebDriver."""
        self.driver = ChromeDriverFactory.create(self.config)
    
    def _navigate_to_page(self, url: str) -> None:
        """Navigate to the product page."""
        self.driver.get(url)
        time.sleep(self.config.page_load_wait_seconds)
    
    def _scroll_to_load_reviews(self) -> None:
        """Scroll down the page to load lazy-loaded reviews."""
        scroll_handler = ReviewScrollHandler(self.driver, self.config)
        scroll_handler.scroll_until_all_loaded()
    
    def _extract_details(self, url: str) -> ProductDetails:
        """Parse the page and extract product details."""
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        parser = ProductPageParser(soup)
        return parser.extract_all(url)
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        if self.driver:
            self.driver.quit()


# =============================================================================
# PUBLIC API
# =============================================================================

def scrape_product_page(
    url: str,
    headless: bool = True,
) -> ProductDetails:
    """
    Scrape an Amazon product page and extract details.
    
    Args:
        url: The full URL of the Amazon product page
        headless: Whether to run the browser in headless mode
    
    Returns:
        ProductDetails containing price, ASIN, and brand
    
    Example:
        >>> details = scrape_product_page("https://www.amazon.com/dp/B09FLNSYDZ")
        >>> print(details.price)
        >>> print(details.asin)
        >>> print(details.brand)
    """
    config = ProductScraperConfig(headless=headless)
    scraper = ProductPageScraper(config)
    return scraper.scrape(url)


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Main entry point for standalone execution."""
    url = 'https://www.amazon.com/Wireless-Bluetooth-Headphones-Earphones-BMANI-VEAT00L/dp/B09FLNSYDZ/ref=zg_bs_g_electronics_d_sccl_99/132-7790363-1367266?th=1'
    
    print("🔍 Scraping product page...")
    details = scrape_product_page(url, headless=True)
    
    print("\n📦 Product Details:")
    print(details)
    
    print("\n⭐ Star Histogram:")
    print(details.star_histogram)
    
    if details.reviews:
        print(f"\n📝 Reviews ({len(details.reviews)} found):")
        for i, review in enumerate(details.reviews, 1):  # Show first 3 reviews
            print(f"\n--- Review {i} ---")
            print(review)


if __name__ == "__main__":
    main()


""""
example of use

from page_scraping import ProductPageScraper, ProductScraperConfig, ProductDetails, Review, StarHistogram

config = ProductScraperConfig(headless=False)  # See the browser
scraper = ProductPageScraper(config)
details = scraper.scrape("https://www.amazon.com/-/es/dp/B09FLNSYDZ/ref=cm_cr_arp_d_btm?ie=UTF8&th=1")

# Access overall product info
print(f"Rating: {details.rating}")
print(f"Total Reviews: {details.total_reviews}")

# Access star histogram
print(f"5-star: {details.star_histogram.five_star}")
print(f"4-star: {details.star_histogram.four_star}")

# Access individual reviews
for review in details.reviews:
    print(f"Review ID: {review.review_id}")
    print(f"Stars: {review.stars}")
    print(f"Date: {review.date}")
    print(f"Location: {review.location}")
    print(f"Title: {review.title}")
    print(f"Text: {review.text}")
    print(f"Verified: {review.verified_purchase}")

# Customize scrolling to load more reviews
config = ProductScraperConfig(
    headless=True,
    scroll_step_pixels=600,  # Smaller steps for slower scrolling
    lazy_load_wait_seconds=3.0,  # Wait longer for slow connections
    max_no_change_attempts=5  # Try more times before giving up
)
scraper = ProductPageScraper(config)
details = scraper.scrape(url)
"""