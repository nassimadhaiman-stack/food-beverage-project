CREATE OR REPLACE VIEW PROMOTIONS_ACTIVES AS
WITH promotions_enriched AS (
    SELECT
        PROMOTION_ID,
        PRODUCT_CATEGORY,
        PROMOTION_TYPE,
        DISCOUNT_RATE,
        START_DATE,
        END_DATE,
        REGION,

        -- Duration of the promotion in days
        DATEDIFF('day', START_DATE, END_DATE)                            AS PROMOTION_DURATION_DAYS,

        -- Discount classification
        CASE
            WHEN DISCOUNT_RATE < 0.10  THEN 'Low Discount'
            WHEN DISCOUNT_RATE < 0.20  THEN 'Medium Discount'
            WHEN DISCOUNT_RATE >= 0.20 THEN 'High Discount'
        END                                                              AS DISCOUNT_CATEGORY,

        -- Promotion status
        CASE
            WHEN '2023-12-30' BETWEEN START_DATE AND END_DATE THEN 'Active'
            WHEN '2023-12-30' < START_DATE                    THEN 'Upcoming'
            WHEN '2023-12-30' > END_DATE                      THEN 'Expired'
        END                                                              AS PROMOTION_STATUS

    FROM  FOOD_BEVERAGE.FB_SILVER.PROMOTION_DATA_CLEAN
),

sales_during_promo AS (
    -- Aggregate transactions that occurred during a promotion window in the same region
    SELECT
        p.PROMOTION_ID,
        COUNT(t.TRANSACTION_ID)                                          AS NB_TRANSACTIONS_DURING_PROMO,
        SUM(CASE WHEN t.TRANSACTION_TYPE = 'SALE' THEN t.AMOUNT ELSE 0 END)  AS TOTAL_SALES_DURING_PROMO,
        AVG(CASE WHEN t.TRANSACTION_TYPE = 'SALE' THEN t.AMOUNT END)    AS AVG_BASKET_DURING_PROMO,
        MIN(t.TRANSACTION_DATE)                                          AS FIRST_TRANSACTION_DATE,
        MAX(t.TRANSACTION_DATE)                                          AS LAST_TRANSACTION_DATE
    FROM FOOD_BEVERAGE.FB_SILVER.PROMOTION_DATA_CLEAN p
    LEFT JOIN FOOD_BEVERAGE.FB_SILVER.FINANCIAL_TRANSACTIONS_CLEAN   t
           ON  t.REGION           = p.REGION
           AND t.TRANSACTION_DATE BETWEEN p.START_DATE AND p.END_DATE
    GROUP BY p.PROMOTION_ID
)

SELECT
    -- Promotion identity
    p.PROMOTION_ID,
    p.REGION,
    p.PRODUCT_CATEGORY,
    p.PROMOTION_TYPE,
    p.PROMOTION_STATUS,

    -- Discount info
    p.DISCOUNT_RATE,
    p.DISCOUNT_CATEGORY,

    -- Timeline
    p.START_DATE,
    p.END_DATE,
    p.PROMOTION_DURATION_DAYS,

    -- Sales impact KPIs
    s.NB_TRANSACTIONS_DURING_PROMO,
    s.TOTAL_SALES_DURING_PROMO,
    ROUND(s.AVG_BASKET_DURING_PROMO, 2)                                  AS AVG_BASKET_DURING_PROMO,
    s.FIRST_TRANSACTION_DATE,
    s.LAST_TRANSACTION_DATE,

    -- Revenue per promo day (efficiency metric)
    ROUND(s.TOTAL_SALES_DURING_PROMO / NULLIF(p.PROMOTION_DURATION_DAYS, 0), 2) AS SALES_PER_PROMO_DAY

FROM promotions_enriched          p
LEFT JOIN sales_during_promo      s ON p.PROMOTION_ID = s.PROMOTION_ID
ORDER BY p.REGION, TOTAL_SALES_DURING_PROMO DESC NULLS LAST;
