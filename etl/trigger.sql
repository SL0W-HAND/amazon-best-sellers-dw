CREATE OR REPLACE FUNCTION core.fn_upsert_product_and_rank()
RETURNS TRIGGER AS $$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
    v_error_hint TEXT;
BEGIN
    -- Validate required fields
    IF NEW.asin IS NULL OR NEW.asin = '' THEN
        RAISE EXCEPTION 'ASIN cannot be null or empty';
    END IF;
    
    IF NEW.product_name IS NULL OR NEW.product_name = '' THEN
        RAISE EXCEPTION 'Product name cannot be null or empty for ASIN: %', NEW.asin;
    END IF;

    -- 1️⃣ Upsert en core.products
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
        updated_at = CURRENT_TIMESTAMP;

    -- 2️⃣ Insert en ranking histórico
    INSERT INTO core.product_rank_history (
        asin,
        category,
        rank_position,
        scraped_at
    )
    VALUES (
        NEW.asin,
        COALESCE(NEW.category, 'unknown'),
        NEW.rank_position,
        COALESCE(NEW.scraped_at, CURRENT_TIMESTAMP)
    );

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
