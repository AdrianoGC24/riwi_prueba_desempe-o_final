-- ============================================================================
-- QUESTION 6: Which products appear in transactions but have inconsistent
--             descriptions? How many unique product codes exist in total?
--
-- Logic: dim_products stores raw_variation_count = number of distinct descriptions
--        found for the same stock_code across the raw CSVs.
--        A product is considered "inconsistent" if raw_variation_count > 1.
-- ============================================================================

-- 6a: Total unique product codes in the analytical repository
SELECT COUNT(*) AS total_unique_product_codes
FROM dim_products;


-- 6b: Products with more than one description variant (inconsistency detected)
SELECT
    stock_code,
    canonical_description,
    category,
    raw_variation_count AS description_variants_found
FROM dim_products
WHERE raw_variation_count > 1
ORDER BY raw_variation_count DESC;


-- 6c: Summary — how many products have inconsistencies vs. consistent ones
SELECT
    CASE
        WHEN raw_variation_count = 1 THEN 'Consistent (1 description)'
        WHEN raw_variation_count BETWEEN 2 AND 3 THEN 'Minor inconsistency (2-3 variants)'
        ELSE 'High inconsistency (4+ variants)'
    END                        AS consistency_level,
    COUNT(*)                   AS product_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_products), 2) AS pct_of_catalog
FROM dim_products
GROUP BY consistency_level
ORDER BY product_count DESC;
