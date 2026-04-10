-- Analyse des tendances de ventes mensuelles par région
SELECT 
    TRUNC(TRANSACTION_DATE, 'MONTH') AS sales_month,
    REGION,
    COUNT(TRANSACTION_ID) AS nb_transactions,
    SUM(AMOUNT) AS total_revenue,
    AVG(AMOUNT) AS average_basket
FROM FOOD_BEVERAGE.FB_SILVER.financial_transactions_clean
WHERE TRANSACTION_TYPE = 'SALE' -- On cible uniquement les ventes
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
