-- Performance en ventes des campagnes de marketing par région et product category
Select 
    M.region, M.product_category,
    Sum(M.BUDGET) as total_money_spent, 
    Round(AVG(M.conversion_rate),3)*100 as avg_conversion_rate,
    SUM(CASE 
        WHEN T.transaction_type = 'SALE' THEN T.amount 
        ELSE 0 
    END) AS total_sales
FROM MARKETING_CAMPAIGNS_CLEAN as M 
LEFT JOIN FINANCIAL_TRANSACTIONS_CLEAN T
    ON M.region = T.region
    AND  T.transaction_date BETWEEN M.start_date AND M.end_date

GROUP BY M.region, M.product_category
Order BY  total_sales desc;

-- Campagnes les plus éfficaces
SELECT 
    CAMPAIGN_TYPE,
    COUNT(*) AS nb_campaigns,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate_pct,
    ROUND(SUM(conversion_rate * reach), 0) AS estimated_conversions,
    SUM(budget) AS total_budget
FROM MARKETING_CAMPAIGNS_CLEAN
GROUP BY CAMPAIGN_TYPE
ORDER BY estimated_conversions desc;

-- qui est le plus touché par ces campagnes
SELECT 
    TARGET_AUDIENCE,
    COUNT(*) AS nb_campaigns,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate_pct,
    ROUND(SUM(conversion_rate * reach), 0) AS estimated_conversions,
    SUM(budget) AS total_budget
FROM MARKETING_CAMPAIGNS_CLEAN
GROUP BY TARGET_AUDIENCE
ORDER BY estimated_conversions desc;

-- Evolution_des_performances_des_campagnes_marketing
SELECT 
    TO_CHAR(start_date, 'YYYY-MM') AS year_month,
    COUNT(*) AS nb_campaigns,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate_pct,
    ROUND(SUM(conversion_rate * reach), 0) AS estimated_conversions,
    SUM(budget) AS total_budget,
      ROUND(SUM(budget) / NULLIF(SUM(conversion_rate * reach), 0), 2) AS cost_per_conversion
FROM MARKETING_CAMPAIGNS_CLEAN
GROUP BY year_month
ORDER BY year_month;

-- 
