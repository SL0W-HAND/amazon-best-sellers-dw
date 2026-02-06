-- ============================================
-- Schema creation
-- ============================================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS core;

CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================
-- Core tables (cleaned/deduplicated data)
-- ============================================
CREATE TABLE
    core.products (
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

CREATE TABLE
    core.product_rank_history (
        id SERIAL PRIMARY KEY,
        asin VARCHAR(20) NOT NULL REFERENCES core.products (asin),
        category TEXT NOT NULL,
        rank_position INT NOT NULL,
        has_price_this_date BOOLEAN NOT NULL DEFAULT FALSE,
        price_on_date NUMERIC(10, 2) DEFAULT NULL,
        scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE
    raw.best_sellers (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        rank_position INT NOT NULL,
        asin VARCHAR(20) NOT NULL,
        product_name TEXT,
        product_url TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE
    raw.product_details (
        id SERIAL PRIMARY KEY,
        asin VARCHAR(20) NOT NULL,
        brand TEXT,
        price NUMERIC(10, 2),
        avg_rating NUMERIC(3, 2),
        total_reviews INT,
        rating_distribution JSONB,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE
    raw.reviews (
        id SERIAL PRIMARY KEY,
        asin VARCHAR(20) NOT NULL,
        review_id VARCHAR(50) NOT NULL,
        rating INT CHECK (rating BETWEEN 1 AND 5),
        review_date DATE,
        country VARCHAR(100),
        title TEXT,
        review_text TEXT,
        verified_purchase BOOLEAN DEFAULT FALSE,
        helpful_count TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT reviews_asin_review_id_unique UNIQUE (asin, review_id)
    );

-- Partitioned table for price history (by month)
CREATE TABLE
    raw.price_history (
        id SERIAL,
        asin VARCHAR(20) NOT NULL,
        price_date DATE NOT NULL,
        price NUMERIC(10, 2),
        source TEXT,
        inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, price_date),
        CONSTRAINT price_history_asin_date_unique UNIQUE (asin, price_date)
    )
PARTITION BY
    RANGE (price_date);

-- Price history partitions for 2026
CREATE TABLE
    raw.price_history_2026_01 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-01-01') TO ('2026-02-01');

CREATE TABLE
    raw.price_history_2026_02 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-02-01') TO ('2026-03-01');

CREATE TABLE
    raw.price_history_2026_03 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-03-01') TO ('2026-04-01');

CREATE TABLE
    raw.price_history_2026_04 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-04-01') TO ('2026-05-01');

CREATE TABLE
    raw.price_history_2026_05 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-05-01') TO ('2026-06-01');

CREATE TABLE
    raw.price_history_2026_06 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-06-01') TO ('2026-07-01');

CREATE TABLE
    raw.price_history_2026_07 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-07-01') TO ('2026-08-01');

CREATE TABLE
    raw.price_history_2026_08 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-08-01') TO ('2026-09-01');

CREATE TABLE
    raw.price_history_2026_09 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-09-01') TO ('2026-10-01');

CREATE TABLE
    raw.price_history_2026_10 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-10-01') TO ('2026-11-01');

CREATE TABLE
    raw.price_history_2026_11 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-11-01') TO ('2026-12-01');

CREATE TABLE
    raw.price_history_2026_12 PARTITION OF raw.price_history FOR
VALUES
FROM
    ('2026-12-01') TO ('2027-01-01');

CREATE INDEX idx_best_sellers_asin ON raw.best_sellers (asin);

CREATE INDEX idx_product_details_asin ON raw.product_details (asin);

CREATE INDEX idx_reviews_asin ON raw.reviews (asin);

CREATE INDEX idx_reviews_asin_review_id ON raw.reviews (asin, review_id);

CREATE INDEX idx_price_history_asin ON raw.price_history (asin);

CREATE INDEX idx_product_rank_history_asin ON core.product_rank_history (asin);

CREATE INDEX idx_product_rank_history_category ON core.product_rank_history (category);

CREATE INDEX idx_price_history_date ON raw.price_history (price_date);