
import os
import sys
import json
import re
import signal
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, NamedTuple
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from extract.page_scraping import (
    scrape_product_page,
    ProductDetails,
    ProductPageScraper,
    ProductScraperConfig,
    Review,
    StarHistogram,
)

# Load environment variables
load_dotenv(Path(__file__).parent.parent.parent / 'credential.env')


# =============================================================================
# DELAY CONFIGURATION (to avoid server blocks)
# =============================================================================

# Random delay range between successful requests (seconds)
DELAY_MIN_SUCCESS = 3.0
DELAY_MAX_SUCCESS = 8.0

# Random delay range after a skip (already updated)
DELAY_MIN_SKIP = 0.5
DELAY_MAX_SKIP = 1.5

# Delay after server error (longer backoff)
DELAY_MIN_SERVER_ERROR = 30.0
DELAY_MAX_SERVER_ERROR = 60.0

# Delay after network error
DELAY_MIN_NETWORK_ERROR = 10.0
DELAY_MAX_NETWORK_ERROR = 20.0

# Delay after other errors
DELAY_MIN_ERROR = 5.0
DELAY_MAX_ERROR = 10.0


def random_delay(min_seconds: float, max_seconds: float, reason: str = "") -> None:
    """Sleep for a random duration between min and max seconds."""
    delay = random.uniform(min_seconds, max_seconds)
    if reason:
        print(f"   ⏳ Waiting {delay:.1f}s {reason}...")
    else:
        print(f"   ⏳ Waiting {delay:.1f}s before next request...")
    time.sleep(delay)


# =============================================================================
# SCRAPING RESULT TYPES
# =============================================================================

class ScrapeStatus(Enum):
    """Status of a scraping attempt."""
    SUCCESS = "success"              # Data scraped and saved
    SKIPPED = "skipped"              # Already updated today
    NO_DATA = "no_data"              # Scraping returned no valid data
    SERVER_ERROR = "server_error"    # Server denied or blocked
    NETWORK_ERROR = "network_error"  # Connection/timeout issues
    PARSE_ERROR = "parse_error"      # Could not parse page
    INTERRUPTED = "interrupted"      # Keyboard interrupt
    UNKNOWN_ERROR = "unknown_error"  # Other errors


@dataclass
class ScrapeResult:
    """Result of a single product scrape attempt."""
    asin: str
    status: ScrapeStatus
    message: str = ""
    price_saved: bool = False
    reviews_added: int = 0
    details_saved: bool = False
    
    @property
    def is_success(self) -> bool:
        return self.status == ScrapeStatus.SUCCESS
    
    @property
    def should_retry(self) -> bool:
        """Whether this error is transient and could be retried."""
        return self.status in (
            ScrapeStatus.SERVER_ERROR,
            ScrapeStatus.NETWORK_ERROR
        )


# Global flag for graceful shutdown
_shutdown_requested = False

def _signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global _shutdown_requested
    _shutdown_requested = True
    print("\n\n⚠️  Interrupt received. Finishing current product and shutting down...")
    print("   (Press Ctrl+C again to force quit)\n")


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'amazon_scraping_db'),
        user=os.getenv('POSTGRES_USER', 'admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123'),
    )


# =============================================================================
# PRODUCT QUERIES
# =============================================================================

def get_products_needing_details(
    conn,
    limit: Optional[int] = 1,
    days_threshold: int = 1
) -> List[Tuple[str, str, str]]:
    """
    Get products that need their details scraped.
    
    Criteria:
    - has_details = FALSE (never scraped)
    - OR last_details_scrape is older than days_threshold
    
    Args:
        conn: Database connection
        limit: Maximum number of products to return. None for all products.
        days_threshold: Rescrape products older than this many days
    
    Returns:
        List of tuples: (asin, product_url, category)
    """
    if limit is None:
        # No limit - get all products needing details
        query = """
            SELECT 
                p.asin,
                p.product_url,
                p.category
            FROM core.products p
            WHERE 
                p.has_details = FALSE
                OR p.last_details_scrape IS NULL
                OR p.last_details_scrape < NOW() - INTERVAL '1 day' * %s
            ORDER BY 
                p.has_details ASC,  -- Prioritize products without details
                p.last_details_scrape ASC NULLS FIRST;
        """
        with conn.cursor() as cur:
            cur.execute(query, (days_threshold,))
            return cur.fetchall()
    else:
        query = """
            SELECT 
                p.asin,
                p.product_url,
                p.category
            FROM core.products p
            WHERE 
                p.has_details = FALSE
                OR p.last_details_scrape IS NULL
                OR p.last_details_scrape < NOW() - INTERVAL '1 day' * %s
            ORDER BY 
                p.has_details ASC,  -- Prioritize products without details
                p.last_details_scrape ASC NULLS FIRST
            LIMIT %s;
        """
        with conn.cursor() as cur:
            cur.execute(query, (days_threshold, limit))
            return cur.fetchall()


def get_single_product_for_testing(conn) -> Optional[Tuple[str, str, str]]:
    """
    Get a single product for testing purposes.
    
    Returns:
        Tuple of (asin, product_url, category) or None
    """
    query = """
        SELECT 
            p.asin,
            p.product_url,
            p.category
        FROM core.products p
        LIMIT 1;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        return result


def was_updated_today(conn, asin: str) -> bool:
    """
    Check if a product was already updated today.
    Enforces the 1-day minimum interval between updates.
    
    Returns:
        True if already updated today, False otherwise
    """
    query = """
        SELECT 1 FROM core.products 
        WHERE asin = %s 
          AND last_details_scrape::DATE = CURRENT_DATE;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (asin,))
        return cur.fetchone() is not None


def get_existing_review_ids(conn, asin: str) -> set:
    """
    Get all existing review IDs for a product.
    Used for incremental review loading.
    
    Returns:
        Set of existing review_id strings
    """
    query = """
        SELECT review_id FROM raw.reviews WHERE asin = %s;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (asin,))
        return {row[0] for row in cur.fetchall()}


def get_products_from_latest_scrape(conn, limit: int = 100) -> List[Tuple[str, str, str]]:
    """
    Get products from the most recent best sellers scrape that need details.
    This is used after scraping top 100 to immediately get their details.
    
    Returns:
        List of tuples: (asin, product_url, category)
    """
    query = """
        SELECT DISTINCT
            p.asin,
            p.product_url,
            p.category
        FROM core.products p
        INNER JOIN raw.best_sellers bs ON p.asin = bs.asin
        WHERE bs.scraped_at = (
            SELECT MAX(scraped_at) FROM raw.best_sellers
        )
        AND (
            p.has_details = FALSE
            OR p.last_details_scrape IS NULL
            OR p.last_details_scrape::DATE < CURRENT_DATE
        )
        ORDER BY p.has_details ASC
        LIMIT %s;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        return cur.fetchall()


# =============================================================================
# DATA PARSING HELPERS
# =============================================================================

def parse_price(price_str: str) -> Optional[Decimal]:
    """Convert price string to Decimal."""
    if not price_str or price_str == 'N/A':
        return None
    
    # Remove currency symbols and clean the string
    cleaned = re.sub(r'[^\d.,]', '', price_str)
    
    # Handle different decimal separators
    if ',' in cleaned and '.' in cleaned:
        # Format like 1,234.56 or 1.234,56
        if cleaned.rindex(',') > cleaned.rindex('.'):
            # European format: 1.234,56
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # US format: 1,234.56
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        # Could be 1,234 or 1,56 - assume comma is decimal if only 2 digits after
        parts = cleaned.split(',')
        if len(parts[-1]) == 2:
            cleaned = cleaned.replace(',', '.')
        else:
            cleaned = cleaned.replace(',', '')
    
    try:
        return Decimal(cleaned)
    except:
        return None


def parse_rating(rating_str: str) -> Optional[Decimal]:
    """Convert rating string to Decimal."""
    if not rating_str or rating_str == 'N/A':
        return None
    
    try:
        # Extract numeric value
        match = re.search(r'(\d+\.?\d*)', rating_str)
        if match:
            return Decimal(match.group(1))
    except:
        pass
    
    return None


def parse_total_reviews(reviews_str: str) -> Optional[int]:
    """Convert total reviews string to integer."""
    if not reviews_str or reviews_str == 'N/A':
        return None
    
    # Remove thousands separators and extract number
    cleaned = re.sub(r'[^\d]', '', reviews_str.replace('.', '').replace(',', ''))
    
    try:
        return int(cleaned)
    except:
        return None


def parse_review_stars(stars_str: str) -> Optional[int]:
    """Convert review stars to integer (1-5)."""
    if not stars_str or stars_str == 'N/A':
        return None
    
    try:
        value = float(stars_str)
        return max(1, min(5, round(value)))
    except:
        return None


def parse_review_date(date_str: str) -> Optional[str]:
    """Parse review date string to ISO format."""
    if not date_str or date_str == 'N/A':
        return None
    
    # Common date patterns
    patterns = [
        (r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', 'es'),  # Spanish: 4 de enero de 2026
        (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', 'en'),  # English: January 4, 2026
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', 'numeric'),  # Numeric: 01/04/2026
    ]
    
    months_es = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    months_en = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    
    for pattern, lang in patterns:
        match = re.search(pattern, date_str.lower())
        if match:
            try:
                if lang == 'es':
                    day, month_name, year = match.groups()
                    month = months_es.get(month_name)
                    if month:
                        return f"{year}-{month:02d}-{int(day):02d}"
                elif lang == 'en':
                    month_name, day, year = match.groups()
                    month = months_en.get(month_name.lower())
                    if month:
                        return f"{year}-{month:02d}-{int(day):02d}"
                elif lang == 'numeric':
                    m, d, y = match.groups()
                    return f"{y}-{int(m):02d}-{int(d):02d}"
            except:
                continue
    
    return None


# =============================================================================
# DATABASE INSERT OPERATIONS
# =============================================================================

def insert_product_details(conn, asin: str, details: ProductDetails) -> bool:
    """
    Insert product details into raw.product_details.
    Uses ON CONFLICT to handle duplicates.
    
    Returns:
        True if successful, False otherwise
    """
    query = """
        INSERT INTO raw.product_details (
            asin,
            brand,
            price,
            avg_rating,
            total_reviews,
            rating_distribution,
            scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                asin,
                details.brand if details.brand != 'N/A' else None,
                parse_price(details.price),
                parse_rating(details.rating),
                parse_total_reviews(details.total_reviews),
                json.dumps(details.star_histogram.to_dict()),
                datetime.now(),
            ))
            result = cur.fetchone()
            conn.commit()
            return result is not None
    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error inserting product details: {e}")
        return False


def insert_price_history(conn, asin: str, price: Decimal) -> bool:
    """
    Insert price into raw.price_history for tracking price changes over time.
    Uses ON CONFLICT to prevent duplicate entries for same day.
    Also updates has_price_history and last_price_scrape in core.products.
    
    Returns:
        True if successful, False otherwise
    """
    if price is None:
        return False
    
    insert_query = """
        INSERT INTO raw.price_history (asin, price_date, price, source)
        VALUES (%s, CURRENT_DATE, %s, 'product_page')
        ON CONFLICT (asin, price_date) DO UPDATE SET
            price = EXCLUDED.price,
            inserted_at = CURRENT_TIMESTAMP;
    """
    
    update_product_query = """
        UPDATE core.products
        SET 
            has_price_history = TRUE,
            last_price_scrape = NOW(),
            updated_at = NOW()
        WHERE asin = %s;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (asin, price))
            cur.execute(update_product_query, (asin,))
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        print(f"  ⚠️ Error inserting price history: {e}")
        return False


def insert_reviews_incremental(conn, asin: str, reviews: List[Review]) -> Tuple[int, int]:
    """
    Insert only NEW reviews into raw.reviews (incremental loading).
    Skips reviews that already exist in the database.
    
    Returns:
        Tuple of (new_reviews_inserted, skipped_existing)
    """
    if not reviews:
        return 0, 0
    
    # Get existing review IDs for this product
    existing_ids = get_existing_review_ids(conn, asin)
    
    # Filter to only new reviews
    new_reviews = [r for r in reviews if r.review_id not in existing_ids]
    skipped = len(reviews) - len(new_reviews)
    
    if not new_reviews:
        return 0, skipped
    
    query = """
        INSERT INTO raw.reviews (
            asin,
            review_id,
            rating,
            review_date,
            country,
            title,
            review_text,
            verified_purchase,
            helpful_count,
            scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin, review_id) DO NOTHING;
    """
    
    inserted_count = 0
    
    for review in new_reviews:
        try:
            with conn.cursor() as cur:
                cur.execute(query, (
                    asin,
                    review.review_id,
                    parse_review_stars(review.stars),
                    parse_review_date(review.date),
                    review.location if review.location != 'N/A' else None,
                    review.title if review.title != 'N/A' else None,
                    review.text if review.text != 'N/A' else None,
                    review.verified_purchase,
                    review.helpful_count if review.helpful_count else None,
                    datetime.now(),
                ))
                conn.commit()
                inserted_count += 1
        except Exception as e:
            conn.rollback()
            print(f"  ⚠️ Error inserting review {review.review_id}: {e}")
    
    return inserted_count, skipped


def insert_reviews(conn, asin: str, reviews: List[Review]) -> int:
    """
    Insert reviews into raw.reviews using the upsert trigger.
    DEPRECATED: Use insert_reviews_incremental instead for better performance.
    
    Returns:
        Number of reviews successfully inserted/updated
    """
    if not reviews:
        return 0
    
    # This query uses the trigger to handle duplicates
    query = """
        INSERT INTO raw.reviews (
            asin,
            review_id,
            rating,
            review_date,
            country,
            title,
            review_text,
            verified_purchase,
            helpful_count,
            scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin, review_id) DO UPDATE SET
            rating = EXCLUDED.rating,
            review_date = EXCLUDED.review_date,
            country = EXCLUDED.country,
            title = EXCLUDED.title,
            review_text = EXCLUDED.review_text,
            verified_purchase = EXCLUDED.verified_purchase,
            helpful_count = EXCLUDED.helpful_count,
            scraped_at = EXCLUDED.scraped_at;
    """
    
    inserted_count = 0
    
    for review in reviews:
        try:
            with conn.cursor() as cur:
                cur.execute(query, (
                    asin,
                    review.review_id,
                    parse_review_stars(review.stars),
                    parse_review_date(review.date),
                    review.location if review.location != 'N/A' else None,
                    review.title if review.title != 'N/A' else None,
                    review.text if review.text != 'N/A' else None,
                    review.verified_purchase,
                    review.helpful_count if review.helpful_count else None,
                    datetime.now(),
                ))
                conn.commit()
                inserted_count += 1
        except Exception as e:
            conn.rollback()
            print(f"  ⚠️ Error inserting review {review.review_id}: {e}")
    
    return inserted_count


def update_product_details_status(conn, asin: str) -> bool:
    """
    Update the product's has_details flag and last_details_scrape timestamp.
    
    Returns:
        True if successful, False otherwise
    """
    query = """
        UPDATE core.products
        SET 
            has_details = TRUE,
            last_details_scrape = NOW(),
            updated_at = NOW()
        WHERE asin = %s;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (asin,))
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error updating product status: {e}")
        return False


# =============================================================================
# MAIN ETL PROCESS
# =============================================================================

def validate_scraped_data(details: ProductDetails) -> Tuple[bool, str]:
    """
    Validate that scraped data contains meaningful information.
    
    Returns:
        Tuple of (is_valid, reason_if_invalid)
    """
    # Check if we got at least some basic info
    has_price = details.price and details.price != 'N/A'
    has_rating = details.rating and details.rating != 'N/A'
    has_brand = details.brand and details.brand != 'N/A'
    has_reviews_count = details.total_reviews and details.total_reviews != 'N/A'
    
    # We need at least price OR rating to consider it valid
    if not has_price and not has_rating:
        return False, "No price or rating found - possible blocked page"
    
    return True, ""


def detect_server_block(details: ProductDetails, error: Optional[Exception] = None) -> bool:
    """
    Detect if the server might have blocked or denied our request.
    
    Common signs:
    - All fields are N/A
    - Captcha page
    - Rate limiting
    """
    if error:
        error_msg = str(error).lower()
        if any(word in error_msg for word in ['captcha', '503', '429', 'rate limit', 'blocked', 'denied']):
            return True
    
    # If all main fields are N/A, likely blocked
    all_na = (
        (not details.price or details.price == 'N/A') and
        (not details.rating or details.rating == 'N/A') and
        (not details.brand or details.brand == 'N/A') and
        (not details.total_reviews or details.total_reviews == 'N/A')
    )
    
    return all_na


def scrape_and_load_product_details(
    asin: str,
    product_url: str,
    conn,
    headless: bool = True,
    force: bool = False
) -> ScrapeResult:
    """
    Scrape a product page and load details into the database.
    Only updates database when valid data is scraped.
    
    Args:
        asin: Product ASIN
        product_url: URL to scrape
        conn: Database connection
        headless: Run browser in headless mode
        force: If True, skip the 1-day check and force update
    
    Returns:
        ScrapeResult with status and details of what was saved
    """
    global _shutdown_requested
    
    print(f"\n🔍 Scraping product: {asin}")
    print(f"   URL: {product_url}")
    
    # Check for shutdown request
    if _shutdown_requested:
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.INTERRUPTED,
            message="Shutdown requested before scraping started"
        )
    
    # Check if already updated today (1-day minimum interval)
    if not force and was_updated_today(conn, asin):
        print(f"   ⏭️ Skipping - already updated today")
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.SKIPPED,
            message="Already updated today"
        )
    
    details = None
    scraper = None
    
    try:
        # Configure scraper for thorough review collection
        config = ProductScraperConfig(
            headless=headless,
            scroll_step_pixels=600,
            lazy_load_wait_seconds=2.5,
            max_no_change_attempts=4
        )
        scraper = ProductPageScraper(config)
        
        # Scrape the product page
        details = scraper.scrape(product_url)
        
        # Check for shutdown after scraping
        if _shutdown_requested:
            print("   ⚠️ Shutdown requested - saving scraped data before exit")
        
        # Validate the scraped data
        is_valid, invalid_reason = validate_scraped_data(details)
        
        if not is_valid:
            # Check if it's a server block
            if detect_server_block(details):
                print(f"   🚫 Server block detected - NOT updating database")
                return ScrapeResult(
                    asin=asin,
                    status=ScrapeStatus.SERVER_ERROR,
                    message="Server block detected - all fields empty"
                )
            else:
                print(f"   ⚠️ Invalid data: {invalid_reason}")
                return ScrapeResult(
                    asin=asin,
                    status=ScrapeStatus.NO_DATA,
                    message=invalid_reason
                )
        
        print(f"\n📦 Scraped Details:")
        print(f"   Price: {details.price}")
        print(f"   Brand: {details.brand}")
        print(f"   Rating: {details.rating}")
        print(f"   Total Reviews: {details.total_reviews}")
        print(f"   Reviews Extracted: {len(details.reviews)}")
        
        # === SAVE TO DATABASE (only if we have valid data) ===
        print("\n💾 Saving to database...")
        
        result = ScrapeResult(
            asin=asin,
            status=ScrapeStatus.SUCCESS,
            message=""
        )
        
        # Insert product details
        if insert_product_details(conn, asin, details):
            print("   ✅ Product details saved")
            result.details_saved = True
        else:
            print("   ⚠️ Failed to save product details")
            # Don't mark as complete failure - we might still save price/reviews
        
        # Insert price history for tracking over time
        price = parse_price(details.price)
        if price and insert_price_history(conn, asin, price):
            print(f"   ✅ Price history saved: ${price}")
            result.price_saved = True
        elif details.price and details.price != 'N/A':
            print(f"   ⚠️ Could not save price: {details.price}")
        
        # Insert only NEW reviews (incremental)
        if details.reviews:
            new_inserted, skipped = insert_reviews_incremental(conn, asin, details.reviews)
            print(f"   ✅ Reviews: {new_inserted} new, {skipped} already existed")
            result.reviews_added = new_inserted
        
        # Only update product status if we saved at least SOME data
        if result.details_saved or result.price_saved:
            if update_product_details_status(conn, asin):
                print("   ✅ Product status updated")
            else:
                print("   ⚠️ Could not update product status")
        else:
            print("   ⚠️ No data saved - NOT updating product status")
            result.status = ScrapeStatus.NO_DATA
            result.message = "Could not save any data to database"
        
        return result
        
    except KeyboardInterrupt:
        print(f"\n   ⚠️ Keyboard interrupt during scraping")
        # If we have partial data, try to save it
        if details and validate_scraped_data(details)[0]:
            print("   📝 Attempting to save partial data...")
            try:
                if insert_product_details(conn, asin, details):
                    print("   ✅ Partial data saved")
            except:
                pass
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.INTERRUPTED,
            message="Keyboard interrupt"
        )
    
    except ConnectionError as e:
        print(f"   🌐 Network error: {e}")
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.NETWORK_ERROR,
            message=str(e)
        )
    
    except TimeoutError as e:
        print(f"   ⏱️ Timeout error: {e}")
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.NETWORK_ERROR,
            message=f"Timeout: {e}"
        )
    
    except Exception as e:
        error_msg = str(e).lower()
        
        # Check for server block indicators
        if any(word in error_msg for word in ['captcha', '503', '429', 'rate', 'blocked', 'denied', 'forbidden']):
            print(f"   🚫 Server denied access: {e}")
            return ScrapeResult(
                asin=asin,
                status=ScrapeStatus.SERVER_ERROR,
                message=str(e)
            )
        
        print(f"   ❌ Error scraping product: {e}")
        return ScrapeResult(
            asin=asin,
            status=ScrapeStatus.UNKNOWN_ERROR,
            message=str(e)
        )


def run_etl_test_single_product(headless: bool = True):
    """
    Run the ETL process for a single product (test mode).
    """
    print("=" * 60)
    print("🚀 Product Details ETL - Test Mode (Single Product)")
    print("=" * 60)
    
    conn = None
    try:
        conn = get_db_connection()
        print("✅ Database connection established")
        
        # Get a single product for testing
        product = get_single_product_for_testing(conn)
        
        if not product:
            print("❌ No products found in database")
            return
        
        asin, product_url, category = product
        print(f"\n📋 Test Product:")
        print(f"   ASIN: {asin}")
        print(f"   Category: {category}")
        print(f"   URL: {product_url}")
        
        # Scrape and load
        result = scrape_and_load_product_details(asin, product_url, conn, headless)
        
        print("\n" + "=" * 60)
        if result.is_success:
            print("✅ ETL Test completed successfully!")
            print(f"   Details saved: {result.details_saved}")
            print(f"   Price saved: {result.price_saved}")
            print(f"   New reviews: {result.reviews_added}")
        elif result.status == ScrapeStatus.SKIPPED:
            print("⏭️ Product was skipped (already updated today)")
        else:
            print(f"⚠️ ETL Test completed with status: {result.status.value}")
            if result.message:
                print(f"   Message: {result.message}")
            
    except Exception as e:
        print(f"❌ ETL Error: {e}")
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")


def run_etl_batch(
    limit: Optional[int] = 10,
    days_threshold: int = 1,
    headless: bool = True
) -> dict:
    """
    Run the ETL process for multiple products.
    Handles keyboard interrupts gracefully.
    
    Args:
        limit: Maximum number of products to process
        days_threshold: Rescrape products older than this many days
        headless: Run browser in headless mode
    
    Returns:
        Dictionary with detailed statistics
    """
    global _shutdown_requested
    _shutdown_requested = False
    
    # Set up signal handlers for graceful shutdown
    original_sigint = signal.signal(signal.SIGINT, _signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, _signal_handler)
    
    print("=" * 60)
    print(f"🚀 Product Details ETL - Batch Mode")
    print(f"   Limit: {'All products' if limit is None else limit} | Days threshold: {days_threshold}")
    print("=" * 60)
    
    stats = {
        "total": 0,
        "success": 0,
        "skipped": 0,
        "no_data": 0,
        "server_errors": 0,
        "network_errors": 0,
        "other_errors": 0,
        "interrupted": False,
        "products_processed": [],
    }
    
    conn = None
    try:
        conn = get_db_connection()
        print("✅ Database connection established")
        
        # Get products needing details (NULL or older than 1 day)
        products = get_products_needing_details(conn, limit, days_threshold)
        stats["total"] = len(products)
        
        if not products:
            print("\n✅ No products need scraping")
            print("   All products have been updated within the last day.")
            return stats
        
        print(f"\n📋 Found {len(products)} products needing details")
        print("   (last_details_scrape is NULL or older than 1 day)")
        print("\n   Press Ctrl+C to stop gracefully after current product\n")
        
        for i, (asin, product_url, category) in enumerate(products, 1):
            # Check for shutdown request
            if _shutdown_requested:
                print(f"\n⚠️ Stopping after product {i-1}/{len(products)} (shutdown requested)")
                stats["interrupted"] = True
                break
            
            print(f"\n[{i}/{len(products)}] Processing: {asin} ({category})")
            
            result = scrape_and_load_product_details(asin, product_url, conn, headless)
            stats["products_processed"].append({
                "asin": asin,
                "status": result.status.value,
                "message": result.message
            })
            
            # Update stats based on result
            if result.status == ScrapeStatus.SUCCESS:
                stats["success"] += 1
                # Random delay after success to appear more human-like
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SUCCESS, DELAY_MAX_SUCCESS)
            elif result.status == ScrapeStatus.SKIPPED:
                stats["skipped"] += 1
                # Short delay even for skips
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SKIP, DELAY_MAX_SKIP, "(skipped)")
            elif result.status == ScrapeStatus.NO_DATA:
                stats["no_data"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_ERROR, DELAY_MAX_ERROR, "(no data)")
            elif result.status == ScrapeStatus.SERVER_ERROR:
                stats["server_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SERVER_ERROR, DELAY_MAX_SERVER_ERROR, "(server error - backing off)")
            elif result.status == ScrapeStatus.NETWORK_ERROR:
                stats["network_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_NETWORK_ERROR, DELAY_MAX_NETWORK_ERROR, "(network error)")
            elif result.status == ScrapeStatus.INTERRUPTED:
                stats["interrupted"] = True
                break
            else:
                stats["other_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_ERROR, DELAY_MAX_ERROR, "(error)")
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 ETL Summary:")
        print(f"   Total queued: {stats['total']}")
        print(f"   ✅ Success: {stats['success']}")
        print(f"   ⏭️ Skipped (already updated): {stats['skipped']}")
        print(f"   📭 No data: {stats['no_data']}")
        print(f"   🚫 Server errors: {stats['server_errors']}")
        print(f"   🌐 Network errors: {stats['network_errors']}")
        print(f"   ❌ Other errors: {stats['other_errors']}")
        if stats["interrupted"]:
            print(f"   ⚠️ Interrupted: Yes")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Keyboard interrupt - shutting down...")
        stats["interrupted"] = True
    except Exception as e:
        print(f"\n❌ ETL Error: {e}")
    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")
    
    return stats


def run_etl_for_latest_top100(headless: bool = True) -> dict:
    """
    Run the ETL process for products from the latest best sellers scrape.
    This should be called right after scraping the top 100 to get their details.
    Handles keyboard interrupts gracefully.
    
    Returns:
        Dictionary with success/error/skipped counts
    """
    global _shutdown_requested
    _shutdown_requested = False
    
    # Set up signal handlers for graceful shutdown
    original_sigint = signal.signal(signal.SIGINT, _signal_handler)
    original_sigterm = signal.signal(signal.SIGTERM, _signal_handler)
    
    print("=" * 60)
    print("🚀 Product Details ETL - Latest Top 100 Products")
    print("=" * 60)
    
    stats = {
        "total": 0,
        "success": 0,
        "skipped": 0,
        "no_data": 0,
        "server_errors": 0,
        "network_errors": 0,
        "other_errors": 0,
        "interrupted": False,
    }
    
    conn = None
    try:
        conn = get_db_connection()
        print("✅ Database connection established")
        
        # Get products from the latest scrape that need details
        products = get_products_from_latest_scrape(conn, limit=100)
        stats["total"] = len(products)
        
        if not products:
            print("\n✅ All products from latest scrape already have details for today")
            return stats
        
        print(f"\n📋 Found {len(products)} products needing details from latest scrape")
        print("\n   Press Ctrl+C to stop gracefully after current product\n")
        
        for i, (asin, product_url, category) in enumerate(products, 1):
            # Check for shutdown request
            if _shutdown_requested:
                print(f"\n⚠️ Stopping after product {i-1}/{len(products)} (shutdown requested)")
                stats["interrupted"] = True
                break
            
            print(f"\n[{i}/{len(products)}] Processing: {asin} ({category})")
            
            result = scrape_and_load_product_details(asin, product_url, conn, headless)
            
            # Update stats based on result and apply random delays
            if result.status == ScrapeStatus.SUCCESS:
                stats["success"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SUCCESS, DELAY_MAX_SUCCESS)
            elif result.status == ScrapeStatus.SKIPPED:
                stats["skipped"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SKIP, DELAY_MAX_SKIP, "(skipped)")
            elif result.status == ScrapeStatus.NO_DATA:
                stats["no_data"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_ERROR, DELAY_MAX_ERROR, "(no data)")
            elif result.status == ScrapeStatus.SERVER_ERROR:
                stats["server_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_SERVER_ERROR, DELAY_MAX_SERVER_ERROR, "(server error - backing off)")
            elif result.status == ScrapeStatus.NETWORK_ERROR:
                stats["network_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_NETWORK_ERROR, DELAY_MAX_NETWORK_ERROR, "(network error)")
            elif result.status == ScrapeStatus.INTERRUPTED:
                stats["interrupted"] = True
                break
            else:
                stats["other_errors"] += 1
                if i < len(products) and not _shutdown_requested:
                    random_delay(DELAY_MIN_ERROR, DELAY_MAX_ERROR, "(error)")
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 ETL Summary:")
        print(f"   Total queued: {stats['total']}")
        print(f"   ✅ Success: {stats['success']}")
        print(f"   ⏭️ Skipped: {stats['skipped']}")
        print(f"   📭 No data: {stats['no_data']}")
        print(f"   🚫 Server errors: {stats['server_errors']}")
        print(f"   🌐 Network errors: {stats['network_errors']}")
        print(f"   ❌ Other errors: {stats['other_errors']}")
        if stats["interrupted"]:
            print(f"   ⚠️ Interrupted: Yes")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Keyboard interrupt - shutting down...")
        stats["interrupted"] = True
    except Exception as e:
        print(f"\n❌ ETL Error: {e}")
    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")
    
    return stats


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Product Details ETL')
    parser.add_argument('--test', action='store_true', help='Run in test mode (single product)')
    parser.add_argument('--latest', action='store_true', help='Process products from latest top 100 scrape')
    parser.add_argument('--all', action='store_true', help='Process all products needing details (no limit)')
    parser.add_argument('--limit', type=int, default=10, help='Max products to process (ignored if --all is used)')
    parser.add_argument('--days', type=int, default=1, help='Rescrape threshold in days')
    parser.add_argument('--visible', action='store_true', help='Show browser window')
    
    args = parser.parse_args()
    
    headless = not args.visible
    
    if args.test:
        run_etl_test_single_product(headless=headless)
    elif args.latest:
        run_etl_for_latest_top100(headless=headless)
    else:
        # Use None for limit if --all flag is set, otherwise use the specified limit
        limit = None if args.all else args.limit
        run_etl_batch(limit=limit, days_threshold=args.days, headless=headless)