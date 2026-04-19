CREATE OR REPLACE VIEW CUSTOMERS_ENRICHED AS
WITH customer_base AS (
    SELECT
        CUSTOMER_ID,
        REGION,
        GENDER,
        MARITAL_STATUS,
        TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30')           AS AGE,
        CASE
            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') < 25              THEN 'Young Adult'
            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') BETWEEN 25 AND 44 THEN 'Adult'
            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') BETWEEN 45 AND 64 THEN 'Middle Aged'
            WHEN TIMESTAMPDIFF(YEAR, DATE_OF_BIRTH, '2023-12-30') >= 65             THEN 'Senior'
        END                                                         AS AGE_GROUP,
        ANNUAL_INCOME,
        CASE
            WHEN ANNUAL_INCOME BETWEEN 20038  AND 64998  THEN 'Low Income'
            WHEN ANNUAL_INCOME BETWEEN 64999  AND 109959 THEN 'Middle Income'
            WHEN ANNUAL_INCOME BETWEEN 109960 AND 154920 THEN 'Upper-Middle Income'
            WHEN ANNUAL_INCOME BETWEEN 154921 AND 199881 THEN 'High Income'
        END                                                         AS INCOME_CATEGORY
    FROM FOOD_BEVERAGE.FB_SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
),

sales_kpis AS (
    -- KPIs at region level (no customer_id in transactions)
    SELECT
        REGION,
        COUNT(*)                                                         AS TOTAL_TRANSACTIONS,
        SUM(CASE WHEN TRANSACTION_TYPE = 'SALE' THEN AMOUNT ELSE 0 END) AS TOTAL_SALES,
        AVG(CASE WHEN TRANSACTION_TYPE = 'SALE' THEN AMOUNT END)        AS AVG_BASKET,
        MAX(TRANSACTION_DATE)                                            AS LAST_PURCHASE_DATE,
        DATEDIFF('day', MAX(TRANSACTION_DATE), '2023-12-30')            AS RECENCY_DAYS
    FROM FOOD_BEVERAGE.FB_SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY REGION
)

SELECT
    -- Identity
    c.CUSTOMER_ID,

    -- Demographics
    c.REGION,
    c.GENDER,
    c.MARITAL_STATUS,

    -- Age
    c.AGE,
    c.AGE_GROUP,

    -- Income
    c.ANNUAL_INCOME,
    c.INCOME_CATEGORY,

    -- Sales KPIs (region level)
    s.TOTAL_TRANSACTIONS,
    s.TOTAL_SALES,
    ROUND(s.AVG_BASKET, 2)  AS AVG_BASKET,
    s.LAST_PURCHASE_DATE,
    s.RECENCY_DAYS

FROM customer_base   c
LEFT JOIN sales_kpis s  ON c.REGION = s.REGION;
