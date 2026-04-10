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
