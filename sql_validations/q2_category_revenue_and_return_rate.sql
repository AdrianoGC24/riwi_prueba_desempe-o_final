-- ============================================================================
-- QUESTION 2: Which product categories generated the most gross revenue
--             and which had the highest return rate?
--
-- Logic: Join fact_daily_product_metrics with dim_products to group by category.
--        Return rate = total returns amount / total gross revenue.
-- ============================================================================

SELECT
    p.category,
    ROUND(SUM(m.gross_revenue)::NUMERIC,  2)                                          AS gross_revenue,
    ROUND(SUM(m.returns_amount)::NUMERIC, 2)                                          AS returns_amount,
    ROUND(SUM(m.net_revenue)::NUMERIC,    2)                                          AS net_revenue,
    SUM(m.sales_count)                                                                 AS total_sales,
    SUM(m.returns_count)                                                               AS total_returns,
    ROUND(
        (SUM(m.returns_amount) / NULLIF(SUM(m.gross_revenue), 0) * 100)::NUMERIC, 2
    )                                                                                  AS return_rate_pct
FROM fact_daily_product_metrics m
JOIN dim_products p ON m.stock_code = p.stock_code
GROUP BY p.category
ORDER BY gross_revenue DESC;

