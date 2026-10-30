-- ============================================================================
-- QUESTION 3a: Top 10 products by net revenue
-- ============================================================================

SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    ROUND(SUM(m.gross_revenue)::NUMERIC,  2) AS gross_revenue,
    ROUND(SUM(m.returns_amount)::NUMERIC, 2) AS returns_amount,
    ROUND(SUM(m.net_revenue)::NUMERIC,    2) AS net_revenue,
    SUM(m.units_sold)                         AS units_sold
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY net_revenue DESC
LIMIT 10;


-- ============================================================================
-- QUESTION 3b: Top 10 products with the highest return rate
--              (min. 10 sale lines to avoid noise from rarely-sold items)
-- ============================================================================

SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    SUM(m.sales_count)                                                                  AS total_sales,
    SUM(m.returns_count)                                                                AS total_returns,
    SUM(m.units_sold)                                                                   AS units_sold,
    SUM(m.units_returned)                                                               AS units_returned,
    ROUND(
        (SUM(m.units_returned)::NUMERIC / NULLIF(SUM(m.units_sold), 0) * 100), 2
    )                                                                                   AS unit_return_rate_pct,
    ROUND(
        (SUM(m.returns_amount)::NUMERIC / NULLIF(SUM(m.gross_revenue), 0) * 100), 2
    )                                                                                   AS revenue_return_rate_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
HAVING SUM(m.sales_count) >= 10
ORDER BY revenue_return_rate_pct DESC
LIMIT 10;

