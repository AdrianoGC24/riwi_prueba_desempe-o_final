-- ============================================================================
-- QUESTION 5: Is there a behavioral difference between identified customers
--             and transactions without a customer ID (UNIDENTIFIED)?
--
-- Decision applied: Transactions without CustomerID were kept and tagged as
-- 'UNIDENTIFIED' in dim_customers (is_identified = FALSE).
-- This query compares key purchase metrics between both segments.
-- ============================================================================

SELECT
    c.is_identified,
    CASE WHEN c.is_identified THEN 'Identified' ELSE 'Unidentified (UNIDENTIFIED)' END AS customer_segment,

    COUNT(DISTINCT t.invoice_no)                                        AS unique_invoices,
    COUNT(*)                                                            AS total_transaction_lines,

    -- Average number of lines per invoice (basket depth)
    ROUND(
        COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT t.invoice_no), 0), 2
    )                                                                   AS avg_lines_per_invoice,

    -- Total and average revenue per invoice
    ROUND(SUM(CASE WHEN t.transaction_type = 'SALE'
                   THEN t.gross_amount ELSE 0 END)::NUMERIC, 2)        AS total_gross_sales,
    ROUND(
        SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(DISTINCT CASE WHEN t.transaction_type = 'SALE'
                                     THEN t.invoice_no END), 0), 2
    )                                                                   AS avg_ticket_per_invoice,

    -- Return rate
    ROUND(
        (SUM(CASE WHEN t.transaction_type = 'RETURN' THEN ABS(t.gross_amount) ELSE 0 END)::NUMERIC
         / NULLIF(SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END), 0)
        ) * 100, 2
    )                                                                   AS return_rate_pct
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
GROUP BY c.is_identified
ORDER BY c.is_identified DESC;

