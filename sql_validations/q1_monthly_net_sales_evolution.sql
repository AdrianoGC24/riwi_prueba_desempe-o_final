-- ============================================================================
-- QUESTION 1: Monthly evolution of net sales (gross sales minus returns)
-- during the full period covered by the data.
--
-- Logic: Net revenue is already pre-calculated in fact_daily_product_metrics.
--        We aggregate it by year + month using dim_date.
-- ============================================================================

SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(m.gross_revenue)::NUMERIC,  2) AS gross_sales,
    ROUND(SUM(m.returns_amount)::NUMERIC, 2) AS total_returns,
    ROUND(SUM(m.net_revenue)::NUMERIC,    2) AS net_sales,
    SUM(m.sales_count)                        AS total_sale_lines,
    SUM(m.returns_count)                      AS total_return_lines
FROM fact_daily_product_metrics m
JOIN dim_date d ON m.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
