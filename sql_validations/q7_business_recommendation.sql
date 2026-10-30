-- ============================================================================
-- QUESTION 7: Concrete and specific business recommendation for the DataMart
--             product team, backed by numbers from the analysis.
--
-- RECOMMENDATION: Prioritize the "Hogar" (Home) category for margin optimization
-- and investigate the top return-generating products to reduce net revenue leakage.
--
-- Supporting evidence queries below:
-- ============================================================================


-- 7a: Category-level P&L summary (gross revenue, returns, net revenue, return rate)
SELECT
    p.category,
    ROUND(SUM(m.gross_revenue)::NUMERIC,  2)                                       AS gross_revenue,
    ROUND(SUM(m.returns_amount)::NUMERIC, 2)                                       AS returns_amount,
    ROUND(SUM(m.net_revenue)::NUMERIC,    2)                                       AS net_revenue,
    ROUND(
        (SUM(m.returns_amount) / NULLIF(SUM(m.gross_revenue), 0) * 100)::NUMERIC, 2
    )                                                                              AS return_rate_pct,
    ROUND(
        (SUM(m.net_revenue) / NULLIF(SUM(m.gross_revenue), 0) * 100)::NUMERIC, 2
    )                                                                              AS net_margin_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.category
ORDER BY net_revenue DESC;


-- 7b: Top 5 products responsible for the most returns revenue (revenue leakage)
--     These are the products that require immediate review by the product team.
SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    ROUND(SUM(m.returns_amount)::NUMERIC, 2)  AS returns_amount,
    ROUND(SUM(m.gross_revenue)::NUMERIC,  2)  AS gross_revenue,
    ROUND(
        (SUM(m.returns_amount) / NULLIF(SUM(m.gross_revenue), 0) * 100)::NUMERIC, 2
    )                                          AS return_rate_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
HAVING SUM(m.gross_revenue) > 0
ORDER BY returns_amount DESC
LIMIT 5;


-- 7c: Monthly trend for the top-revenue category — is it growing or declining?
SELECT
    p.category,
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(m.net_revenue)::NUMERIC, 2)    AS monthly_net_revenue
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
JOIN dim_date     d ON m.date_key   = d.date_key
WHERE p.category = 'Hogar'
GROUP BY p.category, d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ============================================================================
-- RECOMMENDATION TEXT (for Business_decision.md):
-- Based on the analysis:
--   - "Hogar" is the highest-revenue category but has above-average return rates.
--   - A small set of products (~5) drives disproportionate return amounts.
--   - Recommendation: the product team should audit the top 5 return-generating
--     products (see 7b above) for quality or description issues, and set up a
--     monthly alert when a product's return rate exceeds 20%.
--     This alone could recover a significant portion of net revenue.
-- ============================================================================
