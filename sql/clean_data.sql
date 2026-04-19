-- ============================================================
-- Clean Data: Bronze → Silver
-- ============================================================
-- ------------------------------------------------------------
-- 1. Customer Demographics
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.customer_demographics_clean AS
SELECT 
    CUSTOMER_ID,
    -- Suppression des titres (MD, DDS, PhD, Jr, Sr) et espaces en trop
    TRIM(REGEXP_REPLACE(NAME, ' MD| DDS| PhD| Jr\.| Sr\.| IV| III', '')) AS NAME,
    DATE_OF_BIRTH,
    -- Harmonisation des genres vides en 'Other'
    COALESCE(NULLIF(TRIM(GENDER), ''), 'Other') AS GENDER,
    UPPER(TRIM(REGION)) AS REGION,
    INITCAP(TRIM(COUNTRY)) AS COUNTRY,
    INITCAP(TRIM(CITY)) AS CITY,
    COALESCE(NULLIF(TRIM(MARITAL_STATUS), ''), 'Unknown') AS MARITAL_STATUS,
    -- Revenu toujours positif
    ABS(ANNUAL_INCOME) AS ANNUAL_INCOME
FROM Customer_demographics
-- Déduplication : on garde la ligne la plus récente par client
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID 
    ORDER BY DATE_OF_BIRTH DESC
) = 1;

-- ------------------------------------------------------------
-- 2. Customer Service Interactions
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.customer_service_interactions_clean AS
SELECT 
    INTERACTION_ID,
    TO_DATE(INTERACTION_DATE) AS INTERACTION_DATE,
    TRIM(INTERACTION_TYPE) AS INTERACTION_TYPE,
    TRIM(ISSUE_CATEGORY) AS ISSUE_CATEGORY,
    -- Nettoyage des sauts de ligne dans la description
    TRIM(REGEXP_REPLACE(DESCRIPTION, '[\r\n\t]+', ' ')) AS DESCRIPTION,
    DURATION_MINUTES,
    TRIM(RESOLUTION_STATUS) AS RESOLUTION_STATUS,
    FOLLOW_UP_REQUIRED,
    CUSTOMER_SATISFACTION
FROM customer_service_interactions;

-- ------------------------------------------------------------
-- 3. Financial Transactions
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.financial_transactions_clean AS
SELECT 
    TRIM(TRANSACTION_ID) AS TRANSACTION_ID,
    TO_DATE(TRANSACTION_DATE) AS TRANSACTION_DATE,
    UPPER(TRIM(TRANSACTION_TYPE)) AS TRANSACTION_TYPE,
    -- Montant toujours positif
    ABS(AMOUNT) AS AMOUNT,
    UPPER(TRIM(PAYMENT_METHOD)) AS PAYMENT_METHOD,
    UPPER(TRIM(ENTITY)) AS ENTITY,
    UPPER(TRIM(REGION)) AS REGION,
    TRIM(ACCOUNT_CODE) AS ACCOUNT_CODE
FROM Financial_transactions
-- Déduplication par transaction_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY TRANSACTION_DATE DESC) = 1;

-- ------------------------------------------------------------
-- 4. Promotion Data
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.PROMOTION_DATA_CLEAN AS
SELECT 
    PROMOTION_ID,
    UPPER(TRIM(PRODUCT_CATEGORY))  AS PRODUCT_CATEGORY,
    UPPER(TRIM(PROMOTION_TYPE))    AS PROMOTION_TYPE,
    CASE 
        WHEN DISCOUNT_PERCENTAGE > 1 THEN DISCOUNT_PERCENTAGE / 100 
        ELSE DISCOUNT_PERCENTAGE 
    END                            AS DISCOUNT_RATE,
    TO_DATE(START_DATE)            AS START_DATE,
    TO_DATE(END_DATE)              AS END_DATE,
    UPPER(TRIM(REGION))            AS REGION          

FROM PROMOTION_DATA
WHERE TO_DATE(END_DATE) >= TO_DATE(START_DATE)
  AND REGION NOT IN ('0', '1'); 

-- ------------------------------------------------------------
-- 5. Marketing Campaigns
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.marketing_campaigns_clean AS
SELECT 
    CAMPAIGN_ID,
    UPPER(TRIM(CAMPAIGN_NAME)) AS CAMPAIGN_NAME,
    UPPER(TRIM(CAMPAIGN_TYPE)) AS CAMPAIGN_TYPE,
    INITCAP(TRIM(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY,
    UPPER(TRIM(TARGET_AUDIENCE)) AS TARGET_AUDIENCE,
    TO_DATE(START_DATE) AS START_DATE,
    TO_DATE(END_DATE) AS END_DATE,
    UPPER(TRIM(REGION)) AS REGION,
    ABS(CAST(REPLACE(BUDGET, ' ', '') AS NUMBER(15,2))) AS BUDGET,
    ABS(CAST(REPLACE(REACH, ' ', '') AS NUMBER(15,0))) AS REACH,
    ABS(CONVERSION_RATE) AS CONVERSION_RATE
FROM Marketing_campaigns
WHERE TO_DATE(END_DATE) >= TO_DATE(START_DATE);

-- ------------------------------------------------------------
-- 6. Product Reviews
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.product_reviews_clean AS
SELECT 
    REVIEW_ID,
    UPPER(TRIM(PRODUCT_ID)) AS PRODUCT_ID,
    UPPER(TRIM(REVIEWER_ID)) AS REVIEWER_ID,
    INITCAP(TRIM(REVIEWER_NAME)) AS REVIEWER_NAME,
    CAST(RATING AS NUMBER) AS RATING,
    TO_DATE(REVIEW_DATE) AS REVIEW_DATE,
    INITCAP(TRIM(REVIEW_TITLE)) AS REVIEW_TITLE,
    -- Suppression des balises HTML et espaces
    TRIM(REGEXP_REPLACE(REVIEW_TEXT, '<br\s*/?>', ' ')) AS REVIEW_TEXT,
    INITCAP(TRIM(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY
FROM Product_reviews;

-- ------------------------------------------------------------
-- 7. Inventory
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.inventory_clean AS
SELECT 
    UPPER(TRIM(PRODUCT_ID)) AS PRODUCT_ID,
    INITCAP(TRIM(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY,
    UPPER(TRIM(REGION)) AS REGION,
    UPPER(TRIM(COUNTRY)) AS COUNTRY,
    UPPER(TRIM(WAREHOUSE)) AS WAREHOUSE,
    -- Pas de stock négatif
    GREATEST(CURRENT_STOCK, 0) AS CURRENT_STOCK,
    -- Seuil minimum de réapprovisionnement : 50
    GREATEST(REORDER_POINT, 50) AS REORDER_POINT,
    GREATEST(LEAD_TIME, 1) AS LEAD_TIME_DAYS,
    TO_DATE(LAST_RESTOCK_DATE) AS LAST_RESTOCK_DATE
FROM Inventory;

-- ------------------------------------------------------------
-- 8. Store Location
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.store_location_clean AS
SELECT 
    UPPER(TRIM(STORE_ID)) AS STORE_ID,
    INITCAP(TRIM(STORE_NAME)) AS STORE_NAME,
    UPPER(TRIM(STORE_TYPE)) AS STORE_TYPE,
    UPPER(TRIM(REGION)) AS REGION,
    INITCAP(TRIM(COUNTRY)) AS COUNTRY,
    INITCAP(TRIM(CITY)) AS CITY,
    TRIM(ADDRESS) AS ADDRESS,
    TRIM(POSTAL_CODE) AS POSTAL_CODE,
    ABS(SQUARE_FOOTAGE) AS SQUARE_FOOTAGE,
    -- Minimum 1 employé par magasin
    GREATEST(EMPLOYEE_COUNT, 1) AS EMPLOYEE_COUNT
FROM Store_location;

-- ------------------------------------------------------------
-- 9. Logistics & Shipping
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.logistics_shipping_clean AS
SELECT 
    UPPER(TRIM(SHIPMENT_ID)) AS SHIPMENT_ID,
    ORDER_ID,
    TO_DATE(SHIP_DATE) AS SHIP_DATE,
    TO_DATE(ESTIMATED_DELIVERY) AS ESTIMATED_DELIVERY,
    UPPER(TRIM(SHIPPING_METHOD)) AS SHIPPING_METHOD,
    UPPER(TRIM(STATUS)) AS STATUS,
    UPPER(TRIM(CARRIER)) AS CARRIER,
    -- Destination inconnue → 'UNKNOWN'
    COALESCE(NULLIF(UPPER(TRIM(DESTINATION_REGION)), ''), 'UNKNOWN') AS DESTINATION_REGION,
    COALESCE(NULLIF(INITCAP(TRIM(DESTINATION_COUNTRY)), ''), 'UNKNOWN') AS DESTINATION_COUNTRY,
    CAST(SHIPPING_COST AS NUMBER(10,2)) AS SHIPPING_COST
FROM Logistics_and_shipping;

-- ------------------------------------------------------------
-- 10. Supplier Information
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.supplier_information_clean AS
SELECT 
    UPPER(TRIM(SUPPLIER_ID)) AS SUPPLIER_ID,
    INITCAP(TRIM(SUPPLIER_NAME)) AS SUPPLIER_NAME,
    INITCAP(TRIM(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY,
    UPPER(TRIM(REGION)) AS REGION,
    INITCAP(TRIM(COUNTRY)) AS COUNTRY,
    INITCAP(TRIM(CITY)) AS CITY,
    GREATEST(LEAD_TIME, 1) AS LEAD_TIME_DAYS,
    -- Score de fiabilité clampé entre 0 et 1
    CASE 
        WHEN RELIABILITY_SCORE > 1 THEN 1
        WHEN RELIABILITY_SCORE < 0 THEN 0
        ELSE RELIABILITY_SCORE
    END AS RELIABILITY_SCORE,
    UPPER(TRIM(QUALITY_RATING)) AS QUALITY_RATING
FROM Supplier_information;

-- ------------------------------------------------------------
-- 11. Employee Records
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE FOOD_BEVERAGE.FB_SILVER.employee_records_clean AS
SELECT 
    UPPER(TRIM(EMPLOYEE_ID)) AS EMPLOYEE_ID,
    INITCAP(TRIM(NAME)) AS FULL_NAME,
    TO_DATE(DATE_OF_BIRTH) AS DATE_OF_BIRTH,
    TO_DATE(HIRE_DATE) AS HIRE_DATE,
    UPPER(TRIM(DEPARTMENT)) AS DEPARTMENT,
    INITCAP(TRIM(JOB_TITLE)) AS JOB_TITLE,
    LOWER(TRIM(EMAIL)) AS EMAIL,
    UPPER(TRIM(REGION)) AS REGION,
    INITCAP(TRIM(COUNTRY)) AS COUNTRY,
    -- Salaire toujours positif
    ABS(CAST(REPLACE(SALARY, ' ', '') AS NUMBER(12,2))) AS ANNUAL_SALARY
FROM Employee_records;
