-- ============================================
-- Schema creation
-- ============================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;

-- ============================================
-- Core tables (cleaned/deduplicated data)
-- ============================================
CREATE TABLE core.products (
    asin VARCHAR(20) PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_url TEXT NOT NULL,
    category TEXT NOT NULL,

    -- Estado del pipeline
    has_details BOOLEAN NOT NULL DEFAULT FALSE,
    has_price_history BOOLEAN NOT NULL DEFAULT FALSE,
    last_details_scrape TIMESTAMP,
    last_price_scrape TIMESTAMP,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE core.product_rank_history (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) NOT NULL REFERENCES core.products(asin),
    category TEXT NOT NULL,
    rank_position INT NOT NULL,
    scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.best_sellers (
    id SERIAL PRIMARY KEY,
    category TEXT NOT NULL,
    rank_position INT NOT NULL,
    asin VARCHAR(20) NOT NULL,
    product_name TEXT,
    product_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.product_details (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) NOT NULL,
    brand TEXT,
    price NUMERIC(10,2),
    avg_rating NUMERIC(3,2),
    total_reviews INT,
    rating_distribution JSONB,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw.reviews (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    review_date DATE,
    country VARCHAR(50),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw.price_history_images (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) NOT NULL,
    image_path TEXT NOT NULL,
    source TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Partitioned table for price history (by month)
CREATE TABLE raw.price_history (
    id SERIAL,
    asin VARCHAR(20) NOT NULL,
    price_date DATE NOT NULL,
    price NUMERIC(10,2),
    source TEXT,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, price_date)
) PARTITION BY RANGE (price_date);

-- Create partitions for 2025-2026 (add more as needed)
CREATE TABLE raw.price_history_2025_h1 PARTITION OF raw.price_history
    FOR VALUES FROM ('2025-01-01') TO ('2025-07-01');
CREATE TABLE raw.price_history_2025_h2 PARTITION OF raw.price_history
    FOR VALUES FROM ('2025-07-01') TO ('2026-01-01');
CREATE TABLE raw.price_history_2026_h1 PARTITION OF raw.price_history
    FOR VALUES FROM ('2026-01-01') TO ('2026-07-01');
CREATE TABLE raw.price_history_2026_h2 PARTITION OF raw.price_history
    FOR VALUES FROM ('2026-07-01') TO ('2027-01-01');

-- Default partition for dates outside defined ranges
CREATE TABLE raw.price_history_default PARTITION OF raw.price_history DEFAULT;

CREATE INDEX idx_best_sellers_asin ON raw.best_sellers (asin);
CREATE INDEX idx_product_details_asin ON raw.product_details (asin);
CREATE INDEX idx_reviews_asin ON raw.reviews (asin);
CREATE INDEX idx_price_history_asin ON raw.price_history (asin);
CREATE INDEX idx_product_rank_history_asin ON core.product_rank_history (asin);
CREATE INDEX idx_product_rank_history_category ON core.product_rank_history (category);
CREATE INDEX idx_price_history_date ON raw.price_history (price_date);

