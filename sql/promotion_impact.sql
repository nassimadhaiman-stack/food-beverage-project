-- Analyse de l'impact des promotions par catégorie et région
SELECT 
    p.PRODUCT_CATEGORY,
    p.PROMOTION_TYPE,
    p.REGION,
    AVG(p.DISCOUNT_RATE) AS avg_discount_applied,
    COUNT(p.PROMOTION_ID) AS total_promotions_active,
    MIN(p.START_DATE) AS earliest_promo,
    MAX(p.END_DATE) AS latest_promo
FROM FOOD_BEVERAGE.FB_SILVER.promotion_data_clean p
GROUP BY 1, 2, 3
ORDER BY total_promotions_active DESC;
