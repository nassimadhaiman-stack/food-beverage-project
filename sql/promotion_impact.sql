-- Comparaison avec ou sans promotion
WITH daily_stats AS (
    SELECT 
        T.transaction_date,
        T.amount,
        -- On définit si le jour était un jour de promo pour la région
        MAX(CASE WHEN P.promotion_id IS NOT NULL THEN 1 ELSE 0 END) as is_promo_day
    FROM FINANCIAL_TRANSACTIONS_CLEAN T
    LEFT JOIN PROMOTION_DATA_CLEAN P 
        ON T.region = P.region 
        AND T.transaction_date BETWEEN P.start_date AND P.end_date
    WHERE T.transaction_type = 'SALE'
    GROUP BY T.transaction_date, T.amount
),
agg_stats AS (
    SELECT 
        CASE WHEN is_promo_day = 1 THEN 'With Promo' ELSE 'Without Promo' END AS status,
        SUM(amount) as total_rev,
        COUNT(DISTINCT transaction_date) as nb_days -- On compte le nombre de jours réels
    FROM daily_stats
    GROUP BY 1
)
SELECT 
    status,
    total_rev / nb_days AS avg_daily_revenue, -- C'est ce chiffre qu'on compare !
    nb_days
FROM agg_stats;

-- Sensibilité des catégories aux promotions
WITH flagged_sales AS (
    SELECT 
        T.transaction_id,
        T.amount,
        T.region,
        P.PRODUCT_CATEGORY,
        P.PROMOTION_TYPE,
        P.DISCOUNT_RATE,
        CASE WHEN P.promotion_id IS NOT NULL THEN 'With Promo' ELSE 'Without Promo' END AS promo_status
    FROM FOOD_BEVERAGE.FB_SILVER.FINANCIAL_TRANSACTIONS_CLEAN T
    LEFT JOIN FOOD_BEVERAGE.FB_SILVER.PROMOTION_DATA_CLEAN P 
        ON T.region = P.region 
        AND T.transaction_date BETWEEN P.start_date AND P.end_date
    WHERE T.transaction_type = 'SALE'
)
SELECT
    product_category,
    discount_rate,
    region,
    promo_status,
    COUNT(DISTINCT transaction_id) as nb_transactions, 
    SUM(amount) as total_revenue,
    AVG(amount) as average_ticket
FROM flagged_sales
GROUP BY 1,2,3,4
ORDER BY 1, 3;
