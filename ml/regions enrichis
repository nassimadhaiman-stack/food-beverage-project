CREATE OR REPLACE VIEW REGION_ENRICHED AS
WITH region_demographics AS (
    SELECT
        REGION,
        COUNT(DISTINCT CUSTOMER_ID)                                            AS NB_CUSTOMERS,
        ROUND(AVG(ANNUAL_INCOME), 2)                                           AS AVG_INCOME,
        ROUND(AVG(TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30')), 1)       AS AVG_AGE,
        MODE(GENDER)                                                           AS DOMINANT_GENDER,
        MODE(MARITAL_STATUS)                                                   AS DOMINANT_MARITAL_STATUS
    FROM FOOD_BEVERAGE.FB_SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
    GROUP BY REGION
),

region_sales AS (
    SELECT
        REGION,
        COUNT(*)                                                               AS TOTAL_TRANSACTIONS,
        SUM(CASE WHEN TRANSACTION_TYPE = 'SALE' THEN AMOUNT ELSE 0 END)       AS TOTAL_SALES,
        ROUND(AVG(CASE WHEN TRANSACTION_TYPE = 'SALE' THEN AMOUNT END), 2)    AS AVG_BASKET,
        MAX(TRANSACTION_DATE)                                                  AS LAST_PURCHASE_DATE,
        DATEDIFF('day', MAX(TRANSACTION_DATE), '2023-12-30')                  AS RECENCY_DAYS
    FROM FOOD_BEVERAGE.FB_SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY REGION
),

region_promos AS (
    SELECT
        p.REGION,
        COUNT(DISTINCT p.PROMOTION_ID)                                         AS NB_PROMOTIONS,
        ROUND(AVG(p.DISCOUNT_RATE), 4)                                   AS AVG_DISCOUNT_RATE,
        SUM(CASE WHEN t.TRANSACTION_TYPE = 'SALE' THEN t.AMOUNT ELSE 0 END)   AS TOTAL_SALES_DURING_PROMO,
        ROUND(AVG(CASE WHEN t.TRANSACTION_TYPE = 'SALE' THEN t.AMOUNT END), 2) AS AVG_BASKET_DURING_PROMO
    FROM FOOD_BEVERAGE.FB_SILVER.PROMOTION_DATA_CLEAN                  p
    LEFT JOIN FOOD_BEVERAGE.FB_SILVER.FINANCIAL_TRANSACTIONS_CLEAN t
           ON  t.REGION           = p.REGION
           AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
    GROUP BY p.REGION
),

region_campaigns AS (
    SELECT
        REGION,
        COUNT(DISTINCT CAMPAIGN_ID)                                            AS NB_CAMPAIGNS,
        ROUND(SUM(BUDGET), 2)                                                  AS TOTAL_CAMPAIGN_BUDGET,
        ROUND(AVG(CONVERSION_RATE), 4)                                         AS AVG_CONVERSION_RATE,
        SUM(REACH)                                                             AS TOTAL_REACH,
        ROUND(SUM(REACH) / NULLIF(SUM(BUDGET), 0), 4)                         AS REACH_PER_BUDGET,
        MODE(CAMPAIGN_TYPE)                                                    AS DOMINANT_CAMPAIGN_TYPE,
        MODE(TARGET_AUDIENCE)                                                  AS DOMINANT_TARGET_AUDIENCE
    FROM FOOD_BEVERAGE.FB_SILVER.MARKETING_CAMPAIGNS_CLEAN
    GROUP BY REGION
)

SELECT
    -- Identity
    d.REGION,

    -- Customer demographics
    d.NB_CUSTOMERS,
    d.AVG_INCOME,
    d.AVG_AGE,
    d.DOMINANT_GENDER,
    d.DOMINANT_MARITAL_STATUS,

    -- Sales KPIs
    s.TOTAL_TRANSACTIONS,
    s.TOTAL_SALES,
    s.AVG_BASKET,
    s.LAST_PURCHASE_DATE,
    s.RECENCY_DAYS,

    -- Promotion KPIs
    p.NB_PROMOTIONS,
    p.AVG_DISCOUNT_RATE,
    p.TOTAL_SALES_DURING_PROMO,
    p.AVG_BASKET_DURING_PROMO,
    ROUND(p.TOTAL_SALES_DURING_PROMO / NULLIF(s.TOTAL_SALES, 0) * 100, 2)    AS PROMO_SALES_SHARE_PCT,

    -- Campaign KPIs
    c.NB_CAMPAIGNS,
    c.TOTAL_CAMPAIGN_BUDGET,
    c.AVG_CONVERSION_RATE,
    c.TOTAL_REACH,
    c.REACH_PER_BUDGET,
    c.DOMINANT_CAMPAIGN_TYPE,
    c.DOMINANT_TARGET_AUDIENCE,
    ROUND(s.TOTAL_SALES / NULLIF(c.TOTAL_CAMPAIGN_BUDGET, 0), 4)              AS SALES_PER_CAMPAIGN_DOLLAR

FROM region_demographics     d
LEFT JOIN region_sales        s ON d.REGION = s.REGION
LEFT JOIN region_promos       p ON d.REGION = p.REGION
LEFT JOIN region_campaigns    c ON d.REGION = c.REGION
ORDER BY s.TOTAL_SALES DESC NULLS LAST;
