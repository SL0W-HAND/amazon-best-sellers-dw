CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.product_rank_timeseries AS
SELECT
    prh.asin,
    SPLIT_PART(p.product_name, ',', 1) AS product_name,
    prh.category,
    prh.rank_position,
    prh.scraped_at::date AS date
FROM core.product_rank_history prh
JOIN core.products p USING (asin);

CREATE OR REPLACE VIEW analytics.product_price_timeseries AS
SELECT
    asin,
    price_date::date,
    price
FROM raw.price_history;

CREATE OR REPLACE VIEW analytics.product_snapshot AS
SELECT
    p.asin,
    SPLIT_PART(p.product_name, ',', 1) AS product_name,
    p.category,
    d.brand,
    d.price,
    d.avg_rating,
    d.total_reviews,
    p.has_price_history,
    p.has_details
FROM core.products p
LEFT JOIN raw.product_details d USING (asin);

CREATE OR REPLACE VIEW analytics.category_metrics AS
SELECT
    category,
    COUNT(DISTINCT asin) AS products,
    AVG(rank_position) AS avg_rank,
    AVG(price_on_date) AS avg_price
FROM core.product_rank_history
GROUP BY category;

CREATE OR REPLACE VIEW analytics.latest_product_rank AS
SELECT
    prh.asin,
    SPLIT_PART(p.product_name, ',', 1) AS product_name,
    prh.category,
    prh.rank_position,
    prh.scraped_at,
    prh.price_on_date,
    d.brand
FROM core.product_rank_history prh
JOIN core.products p USING (asin)
JOIN raw.product_details d USING (asin)
WHERE prh.scraped_at = (
    SELECT MAX(sub_prh.scraped_at)
    FROM core.product_rank_history sub_prh
    WHERE sub_prh.asin = prh.asin
      AND sub_prh.category = prh.category
);

CREATE OR REPLACE VIEW analytics.times_in_top AS
SELECT 
    t10.asin,
    SPLIT_PART(t10.product_name, ',', 1) AS product_name,
    t10.times_top_10,
    t10.category,
    raw.product_details.brand,
    raw.product_details.price,
    raw.product_details.avg_rating,
    raw.product_details.total_reviews,
    raw.product_details.rating_distribution
FROM (
    SELECT pr.asin, COUNT(*) AS times_top_10, pr.category, SPLIT_PART(p.product_name, ',', 1) AS product_name
    FROM core.product_rank_history pr
    JOIN core.products p ON pr.asin = p.asin
    WHERE rank_position <= 10
    GROUP BY pr.category, pr.asin, p.product_name
    ORDER BY pr.category, times_top_10 DESC
) t10
JOIN raw.product_details ON raw.product_details.asin = t10.asin
AND raw.product_details.scraped_at = (
    SELECT MAX(scraped_at)
    FROM raw.product_details AS pd
    WHERE pd.asin = t10.asin
);



CREATE OR REPLACE VIEW analytics.top_ranked_products_reviews AS
SELECT reviews.asin, raw.reviews.country,t10.product_name, t10.category
FROM analytics.times_in_top t10 
LEFT JOIN raw.reviews ON t10.asin = raw.reviews.asin;