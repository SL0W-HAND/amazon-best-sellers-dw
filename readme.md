# Amazon Data Scraping Pipeline

A complete ETL pipeline for scraping Amazon Best Sellers data and storing it in PostgreSQL. The system extracts product information, details, reviews, and tracks price/rank history over time.

## 📁 Project Structure

```
data_scraping/
├── credential.env              # Database credentials (do not commit!)
├── docker-compose.yml          # PostgreSQL container configuration
├── readme.md                   # This file
├── db/
│   ├── init.sql               # Database schema (tables and indexes)
│   └── trigger.sql            # Trigger for automatic data processing
├── pipelines/
│   ├── run_weekly_etl.py      # 🆕 Master orchestrator for weekly runs
│   ├── extract/
│   │   ├── best_sells_scraping.py   # Best sellers list scraper
│   │   └── page_scraping.py         # Individual product page scraper
│   └── load/
│       ├── load_raw_top_products.py  # ETL: Best sellers → PostgreSQL
│       └── load_products_details.py  # ETL: Product details → PostgreSQL
└── scraping/                   # Additional scraping utilities
```

## ✨ Key Features

- **Weekly Top 100 Tracking**: Scrape top 100 best sellers per category
- **Price History**: Track price changes over time for each product
- **Incremental Reviews**: Only adds NEW reviews (doesn't re-insert existing ones)
- **1-Day Minimum Interval**: Prevents over-scraping by enforcing minimum update intervals
- **Rank History**: Daily snapshots of product rankings
- **Multiple Categories**: Support for 10+ Amazon categories

## 🗄️ Database Schema

### Schemas
- **`raw`**: Raw scraped data (staging area)
- **`core`**: Cleaned and deduplicated data

### Core Tables

| Table | Description |
|-------|-------------|
| `core.products` | Master product catalog with scraping status flags |
| `core.product_rank_history` | Daily ranking history per category |

### Raw Tables

| Table | Description |
|-------|-------------|
| `raw.best_sellers` | Raw best sellers scrape data |
| `raw.product_details` | Product metadata (brand, price, ratings) |
| `raw.reviews` | Customer reviews with ratings |
| `raw.price_history` | Historical price tracking (partitioned by month) |

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ETL PIPELINE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   load_raw_top_products.py                                                  │
│   ========================                                                  │
│   Amazon Best Sellers Page                                                  │
│           │                                                                  │
│           ▼                                                                  │
│   ┌─────────────────┐    TRIGGER    ┌────────────────────────────────────┐ │
│   │ raw.best_sellers├──────────────▶│ core.products                      │ │
│   └─────────────────┘               │ core.product_rank_history          │ │
│                                     └────────────────────────────────────┘ │
│                                                                              │
│   load_products_details.py                                                  │
│   ========================                                                  │
│   Amazon Product Pages                                                      │
│           │                                                                  │
│           ▼                                                                  │
│   ┌───────────────────┐   ┌─────────────┐                                  │
│   │ raw.product_details│   │ raw.reviews │                                  │
│   └───────────────────┘   └─────────────┘                                  │
│           │                      │                                          │
│           └──────────────────────┘                                          │
│                     │                                                        │
│                     ▼                                                        │
│           ┌─────────────────┐                                               │
│           │ core.products    │ (updates has_details flag)                   │
│           └─────────────────┘                                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Chrome browser (for Selenium)
- ChromeDriver

### 1. Configure Credentials

Edit `credential.env` with your desired database credentials:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=amazon_scraping_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

### 2. Start the Database

```bash
# Start PostgreSQL container
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the container
docker-compose down

# Stop and remove all data (fresh start)
docker-compose down -v
```

### 3. Install Python Dependencies

```bash
pip install selenium beautifulsoup4 psycopg2-binary python-dotenv
```

### 4. Run the ETL Pipelines

#### 🆕 Weekly Full Pipeline (Recommended)

The easiest way to run the complete pipeline:

```bash
cd pipelines

# Run weekly ETL for electronics category
python run_weekly_etl.py --category electronics

# Show browser window for debugging
python run_weekly_etl.py --category electronics --visible

# Only scrape best sellers (skip product details)
python run_weekly_etl.py --category toys --skip-details

# List available categories
python run_weekly_etl.py --list-categories
```

**Available Categories:**
| Key | Name |
|-----|------|
| `electronics` | Electronics |
| `computers` | Computers & Accessories |
| `home` | Home & Kitchen |
| `toys` | Toys & Games |
| `books` | Books |
| `fashion` | Clothing, Shoes & Jewelry |
| `sports` | Sports & Outdoors |
| `beauty` | Beauty & Personal Care |
| `health` | Health & Household |
| `automotive` | Automotive |

#### Individual ETL Scripts

##### Scrape Best Sellers Only

```bash
cd pipelines/load
python load_raw_top_products.py
```

This will:
1. Scrape Amazon Best Sellers page (electronics category by default)
2. Insert products into `raw.best_sellers`
3. Trigger automatically populates `core.products` and `core.product_rank_history`

##### Scrape Product Details Only

```bash
cd pipelines/load

# Test mode (single product)
python load_products_details.py --test

# Batch mode (multiple products)
python load_products_details.py --limit 10 --days 1

# Process products from latest top 100 scrape
python load_products_details.py --latest

# Show browser window (debugging)
python load_products_details.py --visible --test
```

Options:
- `--test`: Process only one product (for testing)
- `--latest`: Process products from the most recent best sellers scrape
- `--limit N`: Maximum products to process (default: 10)
- `--days N`: Rescrape products older than N days (default: 1)
- `--visible`: Show browser window instead of headless mode

## ⏰ Scheduling Weekly Runs

### Linux/Mac (cron)

```bash
# Run every Sunday at 2 AM
0 2 * * 0 cd /path/to/data_scraping && python pipelines/run_weekly_etl.py --category electronics >> /var/log/amazon_etl.log 2>&1
```

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Weekly, Sunday, 2:00 AM
4. Action: Start a program
5. Program: `python`
6. Arguments: `pipelines/run_weekly_etl.py --category electronics`
7. Start in: `C:\path\to\data_scraping`

## 🔧 Configuration

### Scraper Settings

Both scrapers can be configured via their respective config classes:

```python
# Best Sellers Scraper
ScraperConfig(
    headless=True,              # Run without browser window
    max_no_change_attempts=3,   # Stop scrolling after 3 unchanged attempts
    scroll_pause_seconds=0.5,   # Pause between scrolls
)

# Product Page Scraper  
ProductScraperConfig(
    headless=True,
    page_load_wait_seconds=1.0,
    scroll_step_pixels=600,
    max_no_change_attempts=4,
)
```

### Changing Categories

To scrape a different category, modify `load_raw_top_products.py`:

```python
etl = BestSellersETL(
    amazon_url="/Best-Sellers-Books/zgbs/books/",
    category="books",
)
```

## 📊 Database Queries

### Check Scraping Progress

```sql
-- Products without details
SELECT COUNT(*) FROM core.products WHERE has_details = FALSE;

-- Products updated today
SELECT COUNT(*) FROM core.products 
WHERE last_details_scrape::DATE = CURRENT_DATE;

-- Recent scrapes
SELECT category, COUNT(*), MAX(scraped_at) 
FROM raw.best_sellers 
GROUP BY category;

-- Ranking history for a product
SELECT * FROM core.product_rank_history 
WHERE asin = 'B09XXXXX' 
ORDER BY scraped_at DESC;
```

### Price History Analysis

```sql
-- Price history for a product
SELECT asin, price_date, price 
FROM raw.price_history 
WHERE asin = 'B09XXXXX'
ORDER BY price_date DESC;

-- Products with price drops in the last week
SELECT DISTINCT ph1.asin, ph1.price as current_price, ph2.price as previous_price,
       (ph2.price - ph1.price) as price_drop
FROM raw.price_history ph1
JOIN raw.price_history ph2 ON ph1.asin = ph2.asin
WHERE ph1.price_date = CURRENT_DATE
  AND ph2.price_date = CURRENT_DATE - INTERVAL '7 days'
  AND ph1.price < ph2.price
ORDER BY price_drop DESC;

-- Average price by product over time
SELECT asin, AVG(price) as avg_price, MIN(price) as min_price, MAX(price) as max_price
FROM raw.price_history
GROUP BY asin;
```

### Product Details

```sql
-- Products with details
SELECT p.asin, p.product_name, d.price, d.avg_rating, d.total_reviews
FROM core.products p
JOIN raw.product_details d ON p.asin = d.asin
WHERE p.has_details = TRUE;

-- Reviews for a product
SELECT * FROM raw.reviews WHERE asin = 'B09XXXXX';

-- Review count by product
SELECT asin, COUNT(*) as review_count 
FROM raw.reviews 
GROUP BY asin 
ORDER BY review_count DESC;

-- New reviews added today
SELECT * FROM raw.reviews 
WHERE scraped_at::DATE = CURRENT_DATE;
```

## 🔄 Trigger Behavior

The `trg_best_sellers_to_core` trigger automatically:

1. **Upserts products** into `core.products` when inserting into `raw.best_sellers`
2. **Records rank history** in `core.product_rank_history` (once per day per product)
3. **Links price data** if product details were scraped the same day

This ensures:
- No duplicate products in core tables
- Daily ranking snapshots
- Automatic price correlation with rankings

## ⚠️ Important Notes

1. **Rate Limiting**: Amazon may block frequent requests. The 1-day minimum interval helps prevent over-scraping.

2. **Selenium Setup**: Ensure ChromeDriver version matches your Chrome browser version.

3. **Data Retention**: The `raw.price_history` table is partitioned by month. Partitions for 2026 are pre-created. For future years, add new partitions:

```sql
-- Example for 2027
CREATE TABLE raw.price_history_2027_01 PARTITION OF raw.price_history
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
```

4. **Credentials**: Never commit `credential.env` to version control. Add it to `.gitignore`.

5. **Incremental Reviews**: The pipeline only inserts NEW reviews. Existing reviews (by `review_id`) are skipped, making re-runs efficient.

6. **1-Day Enforcement**: Products already scraped today are automatically skipped. Use `--force` flag in the code if you need to override this.

## 🔄 Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         WEEKLY ETL PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: run_weekly_etl.py calls load_raw_top_products.py                  │
│  ─────────────────────────────────────────────────────────────────────────  │
│   Amazon Best Sellers Page                                                   │
│           │                                                                  │
│           ▼                                                                  │
│   ┌─────────────────┐    TRIGGER    ┌────────────────────────────────────┐  │
│   │ raw.best_sellers├──────────────▶│ core.products                      │  │
│   └─────────────────┘               │ core.product_rank_history          │  │
│                                     └────────────────────────────────────┘  │
│                                                                              │
│  Step 2: run_weekly_etl.py calls load_products_details.py --latest         │
│  ─────────────────────────────────────────────────────────────────────────  │
│   For each product in latest top 100:                                       │
│           │                                                                  │
│           ├── Check if already updated today → Skip if yes                  │
│           │                                                                  │
│           ▼                                                                  │
│   ┌───────────────────┐   ┌─────────────┐   ┌─────────────────┐            │
│   │ raw.product_details│  │ raw.reviews │   │ raw.price_history│            │
│   │ (latest snapshot)  │  │ (incremental)│   │ (daily prices)  │            │
│   └───────────────────┘   └─────────────┘   └─────────────────┘            │
│           │                      │                   │                       │
│           └──────────────────────┴───────────────────┘                      │
│                     │                                                        │
│                     ▼                                                        │
│           ┌─────────────────┐                                               │
│           │ core.products    │ (updates has_details, last_details_scrape)   │
│           └─────────────────┘                                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📝 License

This project is for educational purposes only. Ensure compliance with Amazon's Terms of Service.