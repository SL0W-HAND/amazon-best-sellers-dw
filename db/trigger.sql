CREATE OR REPLACE FUNCTION core.fn_upsert_product_and_rank()
RETURNS TRIGGER AS $$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
    v_error_hint TEXT;
    v_last_rank_date DATE;
    v_current_date DATE;
    v_is_new_product BOOLEAN;
    v_should_insert_rank BOOLEAN := FALSE;
    v_should_update_product BOOLEAN := FALSE;
    v_price_this_day NUMERIC(10,2) := NULL;
    v_has_price BOOLEAN := FALSE;
    v_scrape_timestamp TIMESTAMP;
    v_last_product_update_date DATE;
BEGIN
    -- Validate required fields
    IF NEW.asin IS NULL OR NEW.asin = '' THEN
        RAISE EXCEPTION 'ASIN cannot be null or empty';
    END IF;
    
    IF NEW.product_name IS NULL OR NEW.product_name = '' THEN
        RAISE EXCEPTION 'Product name cannot be null or empty for ASIN: %', NEW.asin;
    END IF;

    -- Set scrape timestamp
    v_scrape_timestamp := COALESCE(NEW.scraped_at, CURRENT_TIMESTAMP);
    
    -- Get current date
    v_current_date := v_scrape_timestamp::DATE;

    -- Check if product exists and get last update date
    SELECT 
        NOT EXISTS (SELECT 1 FROM core.products WHERE asin = NEW.asin),
        (SELECT updated_at::DATE FROM core.products WHERE asin = NEW.asin)
    INTO v_is_new_product, v_last_product_update_date;

    -- Determine if we should update the product
    IF v_is_new_product THEN
        v_should_update_product := TRUE;
    ELSIF v_last_product_update_date IS NULL OR v_last_product_update_date < v_current_date THEN
        v_should_update_product := TRUE;
    END IF;

    -- 1️⃣ Upsert en core.products only if conditions are met
    IF v_should_update_product THEN
        INSERT INTO core.products (
            asin,
            product_name,
            product_url,
            category,
            created_at,
            updated_at
        )
        VALUES (
            NEW.asin,
            NEW.product_name,
            COALESCE(NEW.product_url, ''),
            COALESCE(NEW.category, 'unknown'),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (asin) DO UPDATE
        SET
            product_name = EXCLUDED.product_name,
            product_url = EXCLUDED.product_url,
            category = EXCLUDED.category,
            updated_at = CURRENT_TIMESTAMP
        WHERE core.products.updated_at::DATE < EXCLUDED.updated_at::DATE;
    END IF;

    -- 2️⃣ Determine if we should insert rank history
    IF v_is_new_product THEN
        -- New product: always insert rank history
        v_should_insert_rank := TRUE;
    ELSE
        -- Existing product: check if last rank update was on a different day
        SELECT scraped_at::DATE
        INTO v_last_rank_date
        FROM core.product_rank_history
        WHERE asin = NEW.asin
        ORDER BY scraped_at DESC
        LIMIT 1;
        
        -- Insert only if different day or no previous rank history
        IF v_last_rank_date IS NULL OR v_last_rank_date < v_current_date THEN
            v_should_insert_rank := TRUE;
        END IF;
    END IF;

    -- 3️⃣ If we should insert rank, check for price data from same day
    IF v_should_insert_rank THEN
        -- Check if there's a price in product_details from the same day
        SELECT price INTO v_price_this_day
        FROM raw.product_details
        WHERE asin = NEW.asin
          AND scraped_at::DATE = v_current_date
        ORDER BY scraped_at DESC
        LIMIT 1;
        
        IF v_price_this_day IS NOT NULL THEN
            v_has_price := TRUE;
        END IF;

        -- Insert into ranking history with price info if available
        INSERT INTO core.product_rank_history (
            asin,
            category,
            rank_position,
            has_price_this_date,
            price_on_date,
            scraped_at
        )
        VALUES (
            NEW.asin,
            COALESCE(NEW.category, 'unknown'),
            NEW.rank_position,
            v_has_price,
            v_price_this_day,
            v_scrape_timestamp
        );
    END IF;

    RETURN NEW;

EXCEPTION
    WHEN unique_violation THEN
        -- Log but don't fail - race condition on concurrent inserts
        RAISE WARNING 'Duplicate key violation for ASIN: %. Skipping...', NEW.asin;
        RETURN NEW;
    
    WHEN foreign_key_violation THEN
        GET STACKED DIAGNOSTICS v_error_detail = PG_EXCEPTION_DETAIL;
        RAISE EXCEPTION 'Foreign key violation for ASIN %: %', NEW.asin, v_error_detail;
    
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS 
            v_error_msg = MESSAGE_TEXT,
            v_error_detail = PG_EXCEPTION_DETAIL,
            v_error_hint = PG_EXCEPTION_HINT;
        
        -- Log the error details
        RAISE WARNING 'Error processing ASIN %: % | Detail: % | Hint: %', 
            NEW.asin, v_error_msg, v_error_detail, v_error_hint;
        
        -- Re-raise to rollback the transaction
        RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_best_sellers_to_core
AFTER INSERT ON raw.best_sellers
FOR EACH ROW
EXECUTE FUNCTION core.fn_upsert_product_and_rank();


-- ============================================
-- Trigger to update core.product_rank_history with price
-- when product details are inserted
-- ============================================
CREATE OR REPLACE FUNCTION core.fn_update_rank_history_price()
RETURNS TRIGGER AS $$
BEGIN
    -- Update the rank history record for the same day with the new price
    UPDATE core.product_rank_history
    SET 
        has_price_this_date = TRUE,
        price_on_date = NEW.price
    WHERE asin = NEW.asin
      AND scraped_at::DATE = NEW.scraped_at::DATE
      AND (has_price_this_date = FALSE OR price_on_date IS NULL);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_product_details_update_price
AFTER INSERT ON raw.product_details
FOR EACH ROW
WHEN (NEW.price IS NOT NULL)
EXECUTE FUNCTION core.fn_update_rank_history_price();
