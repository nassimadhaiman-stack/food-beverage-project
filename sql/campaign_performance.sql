-- Performance des campagnes marketing par type et audience cible
SELECT 
    CAMPAIGN_TYPE,
    TARGET_AUDIENCE,
    PRODUCT_CATEGORY,
    SUM(BUDGET) AS total_investment,
    SUM(REACH) AS total_reach,
    AVG(CONVERSION_RATE) AS avg_conversion_rate,
    -- Calcul de l'efficacité (coût par personne touchée)
    CASE WHEN SUM(REACH) > 0 THEN SUM(BUDGET) / SUM(REACH) ELSE 0 END AS cost_per_reach
FROM FOOD_BEVERAGE.FB_SILVER.marketing_campaigns_clean
GROUP BY 1, 2, 3
ORDER BY avg_conversion_rate DESC;
