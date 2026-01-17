"""
Amazon Best Sellers Scraper
===========================
A modular web scraper for extracting product information from Amazon Best Sellers pages.

Usage:
    python scrap.py
"""

from dataclasses import dataclass, field
from typing import Optional
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class ScraperConfig:
    """Configuration settings for the Amazon scraper."""
    
    # Target URL
    base_url: str = "https://www.amazon.com"
    start_path: str = "/Best-Sellers-Electronics/zgbs/electronics/"
    
    # Browser settings
    headless: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Scroll settings
    scroll_step_pixels: int = 800
    scroll_pause_seconds: float = 0.5
    lazy_load_wait_seconds: float = 2.0
    max_no_change_attempts: int = 3
    
    # Page load settings
    initial_page_load_seconds: float = 3.0
    
    # CSS Selectors (centralized for easy updates)
    product_container_id: str = "gridItemRoot"
    product_name_class: str = "_cDEzb_p13n-sc-css-line-clamp-3_g3dy1"
    product_link_class: str = "a-link-normal"
    next_page_class: str = "a-last"
    
    @property
    def start_url(self) -> str:
        """Returns the full starting URL."""
        return f"{self.base_url}{self.start_path}"


@dataclass
class Product:
    """Represents a scraped product with its details."""
    name: str
    url: str
    image_url: str
    asin: str
    
    def __str__(self) -> str:
        return f"Product(name='{self.name[:50]}...')" if len(self.name) > 50 else f"Product(name='{self.name}')"


@dataclass
class ScrapingResult:
    """Contains the results of a scraping session."""
    products: list[Product] = field(default_factory=list)
    pages_processed: int = 0
    
    @property
    def total_products(self) -> int:
        return len(self.products)
    
    def add_products(self, new_products: list[Product]) -> None:
        """Add products to the result collection."""
        self.products.extend(new_products)
    
    def print_summary(self) -> None:
        """Print a formatted summary of all scraped products."""
        print(f"\n{'='*80}")
        print("RESUMEN DE TODOS LOS PRODUCTOS")
        print('='*80)
        
        for index, product in enumerate(self.products, 1):
            print(f"\n#{index}")
            print(f"  ASIN: {product.asin}")
            print(f"  Nombre: {product.name}")
            print(f"  URL: {product.url}")
            print(f"  Imagen: {product.image_url}")
        
        print(f'\n{"="*80}')
        print(f'TOTAL DE PRODUCTOS ENCONTRADOS: {self.total_products}')
        print(f'PÁGINAS PROCESADAS: {self.pages_processed}')


# =============================================================================
# BROWSER DRIVER
# =============================================================================

class ChromeDriverFactory:
    """Factory for creating configured Chrome WebDriver instances."""
    
    @staticmethod
    def create(config: ScraperConfig) -> webdriver.Chrome:
        """Create and return a configured Chrome WebDriver."""
        options = ChromeDriverFactory._build_options(config)
        return webdriver.Chrome(options=options)
    
    @staticmethod
    def _build_options(config: ScraperConfig) -> Options:
        """Build Chrome options from configuration."""
        chrome_options = Options()
        
        if config.headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(f'--user-agent={config.user_agent}')
        
        return chrome_options


# =============================================================================
# PAGE PARSER
# =============================================================================

class AmazonPageParser:
    """Parses Amazon product pages and extracts product information."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
    
    def parse_products(self, soup: BeautifulSoup) -> list[Product]:
        """Extract all products from a parsed page."""
        product_containers = soup.find_all('div', id=self.config.product_container_id)
        products = []
        
        for container in product_containers:
            product = self._parse_single_product(container)
            if product:
                products.append(product)
        
        return products
    
    def _parse_single_product(self, container) -> Optional[Product]:
        """Parse a single product container and return a Product object."""
        try:
            name = self._extract_product_name(container)
            url = self._extract_product_url(container)
            image_url = self._extract_image_url(container)
            asin = self._extract_asin(container)
            
            return Product(name=name, url=url, image_url=image_url, asin=asin)
        
        except Exception as e:
            print(f"  ⚠ Error al procesar producto: {e}")
            return None
    
    def _extract_product_name(self, container) -> str:
        """Extract the product name from a container."""
        name_element = container.find('div', class_=self.config.product_name_class)
        return name_element.get_text(strip=True) if name_element else 'N/A'
    
    def _extract_product_url(self, container) -> str:
        """Extract the product URL from a container."""
        link_element = container.find('a', class_=self.config.product_link_class)
        
        if link_element and link_element.get('href'):
            return f"{self.config.base_url}{link_element['href']}"
        return 'N/A'
    
    def _extract_image_url(self, container) -> str:
        """Extract the product image URL from a container."""
        img_element = container.find('img')
        return img_element['src'] if img_element and img_element.get('src') else 'N/A'
    
    def _extract_asin(self, container) -> str:
        """Extract the product ASIN from a container."""
        asin_element = container.find('div', attrs={'data-asin': True})
        return asin_element['data-asin'] if asin_element else 'N/A'
    
    def get_next_page_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Find and return the URL of the next page, if available."""
        next_link = soup.find('li', class_=self.config.next_page_class)
        
        if next_link:
            a_tag = next_link.find('a')
            if a_tag and a_tag.get('href'):
                return f"{self.config.base_url}{a_tag['href']}"
        
        return None
    
    def count_products(self, soup: BeautifulSoup) -> int:
        """Count the number of product containers on the page."""
        items = soup.find_all('div', id=self.config.product_container_id)
        return len(items)


# =============================================================================
# SCROLL HANDLER
# =============================================================================

class LazyLoadScrollHandler:
    """Handles scrolling to trigger lazy-loaded content."""
    
    def __init__(self, driver: webdriver.Chrome, config: ScraperConfig):
        self.driver = driver
        self.config = config
        self.parser = AmazonPageParser(config)
    
    def scroll_until_all_loaded(self) -> int:
        """
        Scroll the page gradually to load all lazy-loaded products.
        
        Returns:
            The total number of products found after scrolling.
        """
        print("  📜 Iniciando scroll para cargar todos los productos...")
        
        last_count = 0
        no_change_count = 0
        scroll_position = 0
        
        while no_change_count < self.config.max_no_change_attempts:
            page_height = self._get_page_height()
            scroll_position = self._scroll_down(scroll_position)
            
            if scroll_position >= page_height:
                current_count = self._wait_and_count_products()
                
                if current_count == last_count:
                    no_change_count += 1
                    print(f"     Sin cambios ({no_change_count}/{self.config.max_no_change_attempts})")
                else:
                    no_change_count = 0
                    last_count = current_count
                    print(f"     Productos cargados: {current_count}")
                
                scroll_position = self._update_scroll_position(scroll_position, page_height)
        
        print(f"  ✓ Scroll completado. Productos en página: {last_count}")
        return last_count
    
    def _get_page_height(self) -> int:
        """Get the current page height."""
        return self.driver.execute_script("return document.body.scrollHeight")
    
    def _scroll_down(self, current_position: int) -> int:
        """Scroll down by the configured step amount."""
        new_position = current_position + self.config.scroll_step_pixels
        self.driver.execute_script(f"window.scrollTo(0, {new_position});")
        time.sleep(self.config.scroll_pause_seconds)
        return new_position
    
    def _wait_and_count_products(self) -> int:
        """Wait for lazy loading and count products."""
        time.sleep(self.config.lazy_load_wait_seconds)
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        return self.parser.count_products(soup)
    
    def _update_scroll_position(self, current_position: int, old_height: int) -> int:
        """Update scroll position if the page grew."""
        new_height = self._get_page_height()
        if new_height > old_height:
            return old_height
        return current_position


# =============================================================================
# MAIN SCRAPER
# =============================================================================

class AmazonBestSellersScraper:
    """Main scraper class that orchestrates the scraping process."""
    
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig()
        self.driver: Optional[webdriver.Chrome] = None
        self.parser = AmazonPageParser(self.config)
        self.result = ScrapingResult()
    
    def run(self) -> ScrapingResult:
        """
        Execute the scraping process.
        
        Returns:
            ScrapingResult containing all scraped products.
        """
        try:
            self._initialize_driver()
            self._navigate_to_start_page()
            self._scrape_all_pages()
            
        finally:
            self._cleanup()
        
        return self.result
    
    def _initialize_driver(self) -> None:
        """Initialize the Chrome WebDriver."""
        print("🚀 Inicializando navegador...")
        self.driver = ChromeDriverFactory.create(self.config)
    
    def _navigate_to_start_page(self) -> None:
        """Navigate to the starting URL."""
        print(f"🌐 Navegando a: {self.config.start_url}")
        self.driver.get(self.config.start_url)
        time.sleep(self.config.initial_page_load_seconds)
    
    def _scrape_all_pages(self) -> None:
        """Iterate through all pages and scrape products."""
        current_page = 1
        
        while True:
            self._print_page_header(current_page)
            
            # Load all lazy content
            scroll_handler = LazyLoadScrollHandler(self.driver, self.config)
            scroll_handler.scroll_until_all_loaded()
            
            # Parse the page
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            self._print_page_title(soup)
            
            # Extract products
            page_products = self.parser.parse_products(soup)
            self.result.add_products(page_products)
            self.result.pages_processed = current_page
            
            self._print_page_stats(current_page, len(page_products))
            
            # Check for next page
            next_url = self.parser.get_next_page_url(soup)
            
            if next_url:
                current_page += 1
                self._navigate_to_next_page(next_url, current_page)
            else:
                print("\n✓ No hay más páginas. Fin de la paginación.")
                break
    
    def _print_page_header(self, page_number: int) -> None:
        """Print a formatted header for the current page."""
        print(f"\n{'='*80}")
        print(f"📄 PROCESANDO PÁGINA {page_number}")
        print('='*80)
    
    def _print_page_title(self, soup: BeautifulSoup) -> None:
        """Print the page title."""
        title = soup.title.string if soup.title else "Sin título"
        print(f"  Título: {title}")
    
    def _print_page_stats(self, page_number: int, products_count: int) -> None:
        """Print statistics for the current page."""
        print(f"\n  📊 Productos en página {page_number}: {products_count}")
        print(f"  📊 Total acumulado: {self.result.total_products}")
    
    def _navigate_to_next_page(self, url: str, page_number: int) -> None:
        """Navigate to the next page."""
        print(f"\n  ➡ Navegando a página {page_number}...")
        self.driver.get(url)
        time.sleep(self.config.initial_page_load_seconds)
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        if self.driver:
            print("\n🧹 Cerrando navegador...")
            self.driver.quit()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the scraper."""
    # Create custom configuration (optional)
    config = ScraperConfig(
        headless=True,
        max_no_change_attempts=3,
    )
    
    # Initialize and run the scraper
    scraper = AmazonBestSellersScraper(config)
    result = scraper.run()
    
    # Display results
    result.print_summary()


if __name__ == "__main__":
    main()


