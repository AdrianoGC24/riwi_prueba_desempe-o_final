-- ============================================================================
-- QUESTION 4: Which countries concentrate the most transactions and how does
--             the average ticket (revenue per invoice) vary between them?
--
-- Logic: An "invoice" is a group of transaction lines sharing the same invoice_no.
--        Average ticket = total net revenue / number of unique invoices per country.
--        Only SALE transactions are used for ticket calculation (returns are excluded).
-- ============================================================================

SELECT
    t.country,
    COUNT(*)                                                           AS total_transaction_lines,
    COUNT(DISTINCT t.invoice_no)                                       AS unique_invoices,
    SUM(CASE WHEN t.transaction_type = 'SALE' THEN 1 ELSE 0 END)      AS sale_lines,
    SUM(CASE WHEN t.transaction_type = 'RETURN' THEN 1 ELSE 0 END)    AS return_lines,
    ROUND(SUM(t.gross_amount)::NUMERIC, 2)                             AS total_gross_amount,
    ROUND(
        (SUM(CASE WHEN t.transaction_type = 'SALE' THEN t.gross_amount ELSE 0 END)
         / NULLIF(COUNT(DISTINCT CASE WHEN t.transaction_type = 'SALE'
                                      THEN t.invoice_no END), 0)
        )::NUMERIC, 2
    )                                                                  AS avg_ticket_per_invoice
FROM fact_transactions t
GROUP BY t.country
ORDER BY total_transaction_lines DESC
LIMIT 20;

